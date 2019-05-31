module ParallelTools

using Distributed
include("concurrent_saver.jl")
export ConcurrentSaver, launch_saver, @with_saver

end # module
