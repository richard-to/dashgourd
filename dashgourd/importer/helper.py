import os
import os.path
import subprocess


def run_import_scripts(location, args=[], whitelist=None):
    """Runs import scripts at specified folder location
    
    This is a helper classs in the case that you split import 
    scripts into multiple files. And don't want to manually run 
    each script.
    
    This is helpful if you are evaluating how to import data and 
    what format the data should be in.
    
    Additionally this could be used to update the system in batch 
    instead of sending events as they come.
    
    This assumes the scripts are self contained and will send the 
    appropriate data to dashgourd.
    
    Args:
        location: Can be a single file or a directory. Not recursive.
        args: An optional list of arguments to pass into the script
        whitelist: A list of files to run in the directory.
    """
    
    cmd_args = [0]
    cmd_args.extend(args)

    if os.path.isfile(location):
        path, filename = os.path.split(location)
        label, ext = os.path.splitext(filename)
        if whitelist is None or label in whitelist:
            cmd_args[0] = location 
            subprocess.call(cmd_args)          
    else:
        for dirname, dirnames, filenames in os.walk(location):

            for filename in filenames:
                script = os.path.join(dirname, filename)
                label, ext = os.path.splitext(filename)
                if whitelist is None or label in whitelist:
                    cmd_args[0] = script 
                    subprocess.call(cmd_args)