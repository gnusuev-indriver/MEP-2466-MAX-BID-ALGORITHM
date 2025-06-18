import os
import shutil
from pathlib import Path

def sanitize_heatmaps_folder(PLOT_ROOT_PATH, EXP_ID, essential_list_path="/Users/georgiinusuev/PycharmProjects/work/badbids/exp_anal/SB/src/essential_pics_list.txt"):
    """
    Move essential PNG files from heatmaps folder to heatmaps/essential folder.
    For missing files, check distributions folder and move to distributions/essential.
    
    Parameters:
    - PLOT_ROOT_PATH: Base path for plots
    - EXP_ID: Experiment ID used in file naming
    - essential_list_path: Path to the text file containing list of essential plots
    """
    
    # Define source and destination folders
    heatmaps_source = Path(PLOT_ROOT_PATH) / "heatmaps"
    heatmaps_dest = Path(PLOT_ROOT_PATH) / "heatmaps" / "essential"
    distributions_source = Path(PLOT_ROOT_PATH) / "distributions"
    distributions_dest = Path(PLOT_ROOT_PATH) / "distributions" / "essential"
    
    # Create destination folders if they don't exist
    heatmaps_dest.mkdir(parents=True, exist_ok=True)
    distributions_dest.mkdir(parents=True, exist_ok=True)
    
    # Read the essential pics list
    try:
        with open(essential_list_path, 'r') as f:
            essential_pics = [line.strip() for line in f.readlines() if line.strip()]
    except FileNotFoundError:
        print(f"Essential pics list file not found: {essential_list_path}")
        return
    
    print(f"Found {len(essential_pics)} essential pictures in the list")
    
    moved_from_heatmaps = []
    moved_from_distributions = []
    missing_files = []
    
    # Process each essential picture
    for pic_name in essential_pics:
        # Construct the expected PNG filename
        png_filename = f"{EXP_ID}_{pic_name}.png"
        
        # First, try to find and move from heatmaps folder
        heatmaps_source_path = heatmaps_source / png_filename
        heatmaps_dest_path = heatmaps_dest / png_filename
        
        if heatmaps_source_path.exists():
            try:
                shutil.move(str(heatmaps_source_path), str(heatmaps_dest_path))
                moved_from_heatmaps.append(png_filename)
                print(f"Moved from heatmaps: {png_filename}")
                continue  # File found and moved, skip distributions check
            except Exception as e:
                print(f"Error moving {png_filename} from heatmaps: {e}")
        
        # If not found in heatmaps, try distributions folder
        distributions_source_path = distributions_source / png_filename
        distributions_dest_path = distributions_dest / png_filename
        
        if distributions_source_path.exists():
            try:
                shutil.move(str(distributions_source_path), str(distributions_dest_path))
                moved_from_distributions.append(png_filename)
                print(f"Moved from distributions: {png_filename}")
            except Exception as e:
                print(f"Error moving {png_filename} from distributions: {e}")
                missing_files.append(png_filename)
        else:
            missing_files.append(png_filename)
    
    # Print summary
    print(f"\n=== Summary ===")
    print(f"Successfully moved from heatmaps: {len(moved_from_heatmaps)} files")
    print(f"Successfully moved from distributions: {len(moved_from_distributions)} files")
    print(f"Total moved: {len(moved_from_heatmaps) + len(moved_from_distributions)} files")
    print(f"Missing files: {len(missing_files)} files")
    
    if missing_files:
        print(f"\nFiles not found in either location:")
        for missing in missing_files:
            print(f"  - {missing}")
    
    # List remaining files in both source folders
    remaining_heatmaps = [f for f in heatmaps_source.glob("*.png") if f.is_file()]
    remaining_distributions = [f for f in distributions_source.glob("*.png") if f.is_file()] if distributions_source.exists() else []
    
    print(f"\nRemaining PNG files:")
    print(f"  - In heatmaps folder: {len(remaining_heatmaps)}")
    print(f"  - In distributions folder: {len(remaining_distributions)}")
    
    return {
        'moved_from_heatmaps': moved_from_heatmaps,
        'moved_from_distributions': moved_from_distributions,
        'missing_files': missing_files,
        'remaining_heatmaps': [f.name for f in remaining_heatmaps],
        'remaining_distributions': [f.name for f in remaining_distributions]
    }
