#!/usr/bin/env python3
"""
Run all Python scripts in a directory
"""
import os
import sys
import subprocess
import glob
from pathlib import Path

def run_scripts(directory=".", pattern="*.py", exclude=None):
    """
    Run all Python scripts in a directory
    
    Args:
        directory: Directory to search for scripts
        pattern: File pattern to match (default: *.py)
        exclude: List of script names to exclude
    """
    exclude = exclude or []
    
    # Find all Python scripts
    scripts = sorted(glob.glob(os.path.join(directory, pattern)))
    
    # Filter out excluded scripts
    scripts = [s for s in scripts if os.path.basename(s) not in exclude]
    
    if not scripts:
        print(f"No scripts found matching {pattern} in {directory}")
        return
    
    print(f"Found {len(scripts)} scripts to run:")
    for script in scripts:
        print(f"  - {script}")
    print()
    
    # Run each script
    for i, script in enumerate(scripts, 1):
        print("=" * 80)
        print(f"Running script {i}/{len(scripts)}: {script}")
        print("=" * 80)
        
        try:
            result = subprocess.run(
                [sys.executable, script],
                check=True,
                capture_output=False
            )
            print(f"✓ {script} completed successfully\n")
            
        except subprocess.CalledProcessError as e:
            print(f"✗ {script} failed with exit code {e.returncode}\n")
            # Optionally stop on first failure
            # sys.exit(1)
        
        except KeyboardInterrupt:
            print(f"\n\nInterrupted by user")
            sys.exit(1)
    
    print("=" * 80)
    print("All scripts completed!")

if __name__ == "__main__":
    # Example: Run all scripts in 'scripts/' folder
    # Exclude the runner script itself and cleanup script
    run_scripts(
        directory="gold/scripts/py/",
        pattern="*.py",
        exclude=["run_all.py", "__init__.py", "00_cleanup_db.py"]
    )