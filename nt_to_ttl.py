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

if __name__ == "__main__":
    if len(sys.argv) < 2:
        print("Usage: python nt_to_ttl.py <input_file1.nt> [<input_file2.nt> ...]")
        sys.exit(1)
    
    for input_file in sys.argv[1:]:
        convert_nt_to_ttl(input_file)
