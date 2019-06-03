using ProgressMeter, JLD2, MacroTools

abstract type AbstractSaver end

mutable struct ConcurrentSaver{R<:RemoteChannel} <: AbstractSaver
    readout_channel::R
    process_id::Int
    rem_packets::Int
    active::Bool
    progressbar::Bool
    fpath::String
end

#function ConcurrentSaver(r::RemoteChannel, pid::Int, )
# TO DO: check that the remote channel points to the right process (readout_ch.id == s.process_id)

#=
mutable struct ConcurrentSaver{:indefinite_load_size} <: AbstractSaver
    readout_channel::RemoteChannel
    process_id::Int
    active::Bool
    progressbar::Bool
    fpath::String
end
=#

function launch_saver!(s::ConcurrentSaver{R}) where {R<:RemoteChannel}
    @spawnat s.process_id _launch_saver(s)
end

function _launch_saver!(s::ConcurrentSaver{R}) where {R<:RemoteChannel}
    Npackets::Int = s.rem_packets

    if s.progressbar
        # Set up a progress bar
        progress = Progress(Npackets);
        ProgressMeter.update!(progress, 0);
    end

    jldopen(s.fpath, "a+") do file
        println("Saving data to ",s.fpath)

        while s.rem_packets > 0
            # Retrieve a queued result
            n, sol = take!(s.readout_channel);
            s.rem_packets -= 1;
            for (key, val) in sol
                file["$n/" * key] = val;
            end
            s.progressbar ? ProgressMeter.next!(progress) : nothing;
        end
        # Set progress bar to 100%
        if s.progressbar && (progress.counter < Npackets) ProgressMeter.update!(progress, Npackets); end;
    end

    s.active = false

    nothing
end;

macro with_saver(s, expr)
    @assert isa(expr, Expr) "Not an expression."
    @assert  expr.head == :do

    call = first(expr.args)
    @assert call.head == :call && call.args[1] == :pmap
    args   = call.args[2:end][typeof.(call.args[2:end]) .== Symbol]
    kwargs = call.args[2:end][typeof.(call.args[2:end]) .== Expr]

    wp = Symbol()
    iterable = Symbol()
    if length(args) == 2
        wp, iterable = args
    elseif length(args) == 1
        wp = :(CachingPool(workers()))
        iterable = args[1]
    else
        throw(error("WTF are these function arguments? $(length(args))"))
    end

    index = expr.args[end].args[1].args[]
    old_inner_expr = expr.args[end].args[end]
    rvars = []
    if length(old_inner_expr.args[end].args) > 1  # No return keyword was used
        rvars = old_inner_expr.args[end].args
    else                                          # return was used
        rvars = old_inner_expr.args[end].args[].args
    end
    rexpr = Expr(:tuple, :i, Expr(:call, :Dict, [Expr(:call,:(=>),:($rv_str), rv) for (rv_str, rv) in zip(string.(rvars), rvars)]...))

    quote
        @sync begin
            @spawnat s.process_id ParallelTools._launch_saver!(s)
            @sync pmap($wp,eachindex($iterable)) do i
                put!(s.readout_channel,begin
                    $index = $iterable[i]
                    $(old_inner_expr.args[1:end-1]...)
                    $rexpr
                end);
                nothing
            end
        end
        nothing
    end |> esc |> prettify
end
