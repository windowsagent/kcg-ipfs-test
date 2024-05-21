import argparse
import requests
import csv

def get_ls(node_address, cid):
    response = requests.post(f"http://{node_address}/api/v0/ls", params={'arg': cid})
    response.raise_for_status()
    return response.json()

def get_stat(node_address, cid):
    response = requests.post(f"http://{node_address}/api/v0/files/stat", params={'arg': f'/ipfs/{cid}'})
    response.raise_for_status()
    return response.json()

def traverse_directory(node_address, cid, parent_cid, csv_writer, visited, row_index, parent_index):
    if cid in visited:
        return row_index
    visited.add(cid)

    ls_data = get_ls(node_address, cid)
    for obj in ls_data['Objects']:
        for link in obj['Links']:
            name = link['Name']
            child_cid = link['Hash']
            size = link['Size']
            type_str = 'directory' if link['Type'] == 1 else 'file'

            # Get detailed stats for the current CID
            stat_data = get_stat(node_address, child_cid)
            cumulative_size = stat_data['CumulativeSize']
            blocks = stat_data['Blocks']

            # Write the current entry to the CSV
            csv_writer.writerow({
                'name': name,
                'type': type_str,
                'cid': child_cid,
                'size': size,
                'cumulative size': cumulative_size,
                'blocks': blocks,
                'parent_cid': parent_cid,
                'index_of_parent': parent_index
            })

            current_index = row_index
            row_index += 1

            # If the current link is a directory, traverse it recursively
            if type_str == 'directory':
                row_index = traverse_directory(node_address, child_cid, cid, csv_writer, visited, row_index, row_index)
    
    return row_index

def main():
    parser = argparse.ArgumentParser(description="IPFS CSV dumper")
    parser.add_argument("-i", "--root-cid", default="bafyb4iadbza7ckc3djc2k5lfaorwaufcjurzxzkjsj5e7qt2wrguqs7ywm", help="Root CID in IPFS")
    parser.add_argument("--node-address", default="127.0.0.1:5001", help="IPFS node address {ip_address}:{port}")
    parser.add_argument("-o", "--output", default="cid_crawl_output.csv", help="Output CSV file")
    args = parser.parse_args()

    with open(args.output, 'w', newline='') as csvfile:
        fieldnames = ['name', 'type', 'cid', 'size', 'cumulative size', 'blocks', 'parent_cid', 'index_of_parent']
        csv_writer = csv.DictWriter(csvfile, fieldnames=fieldnames)
        csv_writer.writeheader()

        visited = set()
        # Initialize traversal with the root CID and a starting index of 0
        traverse_directory(args.node_address, args.root_cid, args.root_cid, csv_writer, visited, 1, 0)

if __name__ == "__main__":
    main()