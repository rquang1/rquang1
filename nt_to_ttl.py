import sys
import os
from rdflib import Graph

def convert_nt_to_ttl(input_nt_file):
    if not os.path.isfile(input_nt_file):
        print(f"Error: File '{input_nt_file}' not found.")
        return
    
    output_ttl_file = os.path.splitext(input_nt_file)[0] + "_ttl.ttl"
    
    g = Graph()
    g.parse(input_nt_file, format='nt')  # Parse the .nt file
    g.serialize(destination=output_ttl_file, format='turtle')  # Serialize as .ttl
    
    print(f"Conversion complete: {output_ttl_file}")

def convert_directory(directory):
    if not os.path.isdir(directory):
        print(f"Error: Directory '{directory}' not found.")
        return
    
    nt_files = [f for f in os.listdir(directory) if f.endswith('.nt')]
    if not nt_files:
        print("No .nt files found in the directory.")
        return
    
    for nt_file in nt_files:
        convert_nt_to_ttl(os.path.join(directory, nt_file))

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python nt_to_ttl.py <input_file1.nt> [<input_file2.nt> ...] or python nt_to_ttl.py <directory>")
        sys.exit(1)
    
    input_path = sys.argv[1]
    
    if os.path.isdir(input_path):
        convert_directory(input_path)
    else:
        for input_file in sys.argv[1:]:
            convert_nt_to_ttl(input_file)
