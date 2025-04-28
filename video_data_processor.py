"""
video_data_processor.py

A module for processing video data, including:
- Converting Stata files to Parquet format
- Loading and processing video viewing data
- Filtering videos based on time differences

Author: Tonglin Zhang
Date: Feb 20, 2025
Version: 1.0.0
"""

import pandas as pd
import pyarrow.parquet as pq
from tqdm import tqdm
from typing import Optional, List

def convert_stata_to_parquet(
    input_path: str, 
    output_path: str, 
    chunksize: int = 30000
) -> None:
    """
    Convert Stata file to Parquet format
    
    Args:
        input_path: Path to the Stata file
        output_path: Path for the output Parquet file
        chunksize: Number of rows to read at a time
    """
    try:
        with tqdm(desc="Converting Progress", unit="rows") as pbar:
            chunks = []
            for chunk in pd.read_stata(input_path, chunksize=chunksize):
                chunks.append(chunk)
                pbar.update(len(chunk))
            
            pd.concat(chunks).to_parquet(output_path, index=False)
        print(f"File successfully converted to: {output_path}")
    except Exception as e:
        print(f"Error during conversion: {str(e)}")

def load_parquet_data(file_path: str) -> pd.DataFrame:
    """
    Load data from Parquet file
    
    Args:
        file_path: Path to the Parquet file
    
    Returns:
        pandas DataFrame
    """
    try:
        table = pq.read_table(file_path)
        return table.to_pandas()
    except Exception as e:
        print(f"Error reading Parquet file: {str(e)}")
        return None

def process_video_data(
    df: pd.DataFrame,
    min_days: int = 30,
    exclude_negative_diff: bool = True
) -> pd.DataFrame:
    """
    Process video data, calculate time differences and filter according to conditions
    
    Args:
        df: Input DataFrame
        min_days: Minimum number of days required
        exclude_negative_diff: Whether to exclude negative time differences
    
    Returns:
        Processed DataFrame
    """
    try:
        # Calculate time difference
        df = df.copy()
        df['time_diff'] = df['date'] - df['pub']
        
        # Exclude negative time differences (if required)
        if exclude_negative_diff:
            df = df[df['time_diff'] != pd.Timedelta(days=-1)]
        
        # Calculate maximum time difference for each video and filter valid videos
        max_time_diff = df.groupby('avid')['time_diff'].max()
        valid_videos = max_time_diff[max_time_diff >= pd.Timedelta(days=min_days)]
        
        # Filter final data
        return df[df['avid'].isin(valid_videos.index)]
    
    except Exception as e:
        print(f"Error processing data: {str(e)}")
        return None

def main(
    stata_file: str,
    parquet_file: str,
    min_days: int = 30,
    chunksize: int = 30000
) -> Optional[pd.DataFrame]:
    """
    Main function integrating all processing steps
    
    Args:
        stata_file: Path to Stata file
        parquet_file: Path to Parquet file
        min_days: Minimum days requirement
        chunksize: Chunk size for data reading
    
    Returns:
        Processed DataFrame
    """
    # Convert file format
    convert_stata_to_parquet(stata_file, parquet_file, chunksize)
    
    # Load data
    df = load_parquet_data(parquet_file)
    if df is None:
        return None
    
    # Process data
    result_df = process_video_data(df, min_days)
    return result_df


if __name__ == "__main__":
    
    # Set file paths
    stata_file = "/Users/iuser/Downloads/sampled_avid.dta"
    parquet_file = "sampled_avid.parquet"
    output_file = "cleaned_data.csv"

    # Run main program
    result = main(stata_file, parquet_file)

    if result is not None:
        print("Data processing completed")
        print(f"Number of rows after processing: {len(result)}")
        result.to_csv(output_file)
