import argparse
import requests
import csv
import logging
from requests.exceptions import RequestException

# Configure logging
logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(levelname)s - %(message)s')

def get_ls(node_address, cid):
    """Fetches the list of links for a given CID from the IPFS node."""
    try:
        response = requests.post(f"http://{node_address}/api/v0/ls", params={'arg': cid})
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Failed to get ls for CID {cid}: {e}")
        raise

def get_stat(node_address, cid):
    """Fetches the statistics for a given CID from the IPFS node."""
    try:
        response = requests.post(f"http://{node_address}/api/v0/files/stat", params={'arg': f'/ipfs/{cid}'})
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Failed to get stat for CID {cid}: {e}")
        raise

def get_dag(node_address, cid):
    """Fetches the DAG data for a given CID from the IPFS node."""
    try:
        response = requests.post(f"http://{node_address}/api/v0/dag/get", params={'arg': f'/ipfs/{cid}'})
        response.raise_for_status()
        return response.json()
    except RequestException as e:
        logging.error(f"Failed to get dag for CID {cid}: {e}")
        raise

def traverse_dag(node_address, cid, parent_cid, dag_csv_writer, visited):
    """Recursively traverses a DAG node and writes data to a CSV file."""
    if cid in visited:
        return
    visited.add(cid)

    try:
        dag_data = get_dag(node_address, cid)
    except Exception as e:
        logging.error(f"Skipping DAG CID {cid} due to error: {e}")
        return

    if isinstance(dag_data, dict) and 'Links' in dag_data:
        links = dag_data['Links']
        num_links = len(links)
        final_block_hash = links[-1]['Hash']['/'] if num_links > 0 else None

        for link in links:
            link_cid = link['Hash']['/']
            link_size = link['Tsize']
            dag_csv_writer.writerow({
                'cid': cid,
                'parent_cid': parent_cid,
                'final_block_hash': final_block_hash,
                'num_links': num_links,
                'size': link_size
            })
            traverse_dag(node_address, link_cid, cid, dag_csv_writer, visited)
    else:
        # This is the final block
        dag_csv_writer.writerow({
            'cid': cid,
            'parent_cid': parent_cid,
            'final_block_hash': cid,
            'num_links': 0,
            'size': 0
        })

def traverse_directory(node_address, cid, parent_cid, dir_csv_writer, file_csv_writer, dag_csv_writer, visited, row_index, parent_index):
    """Recursively traverses an IPFS directory and writes data to CSV files."""
    if cid in visited:
        return row_index
    visited.add(cid)

    try:
        ls_data = get_ls(node_address, cid)
    except Exception as e:
        logging.error(f"Skipping CID {cid} due to error: {e}")
        return row_index

    if 'Objects' not in ls_data:
        logging.warning(f"No 'Objects' in ls_data for CID {cid}")
        return row_index

    for obj in ls_data['Objects']:
        for link in obj.get('Links', []):
            name = link.get('Name')
            child_cid = link.get('Hash')
            size = link.get('Size')
            type_str = 'directory' if link.get('Type') == 1 else 'file'

            try:
                stat_data = get_stat(node_address, child_cid)
            except Exception as e:
                logging.error(f"Skipping child CID {child_cid} due to error: {e}")
                continue

            cumulative_size = stat_data.get('CumulativeSize')
            blocks = stat_data.get('Blocks')

            current_index = row_index
            row_index += 1

            csv_writer = dir_csv_writer if type_str == 'directory' else file_csv_writer
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

            # Traverse the DAG for this CID
            traverse_dag(node_address, child_cid, cid, dag_csv_writer, visited)

            if type_str == 'directory':
                row_index = traverse_directory(node_address, child_cid, cid, dir_csv_writer, file_csv_writer, dag_csv_writer, visited, row_index, current_index)

    return row_index

def main():
    parser = argparse.ArgumentParser(description="IPFS CSV dumper")
    parser.add_argument("-i", "--root-cid", default="bafyb4iadbza7ckc3djc2k5lfaorwaufcjurzxzkjsj5e7qt2wrguqs7ywm", help="Root CID in IPFS")
    parser.add_argument("--node-address", default="127.0.0.1:5001", help="IPFS node address {ip_address}:{port}")
    parser.add_argument("-do", "--dir-output", default="directories_output.csv", help="Output CSV file for directories")
    parser.add_argument("-fo", "--file-output", default="files_output.csv", help="Output CSV file for files")
    parser.add_argument("-bo", "--block-output", default="blocks_output.csv", help="Output CSV file for blocks")
    args = parser.parse_args()

    with open(args.dir_output, 'w', newline='') as dir_csvfile, open(args.file_output, 'w', newline='') as file_csvfile, open(args.block_output, 'w', newline='') as block_csvfile:
        fieldnames = ['name', 'type', 'cid', 'size', 'cumulative size', 'blocks', 'parent_cid', 'index_of_parent']
        dir_csv_writer = csv.DictWriter(dir_csvfile, fieldnames=fieldnames)
        file_csv_writer = csv.DictWriter(file_csvfile, fieldnames=fieldnames)
        dir_csv_writer.writeheader()
        file_csv_writer.writeheader()

        block_fieldnames = ['cid', 'parent_cid', 'final_block_hash', 'num_links', 'size']
        dag_csv_writer = csv.DictWriter(block_csvfile, block_fieldnames)
        dag_csv_writer.writeheader()

        visited = set()
        traverse_directory(args.node_address, args.root_cid, args.root_cid, dir_csv_writer, file_csv_writer, dag_csv_writer, visited, 1, 0)

if __name__ == "__main__":
    main()