import gzip
import json
from typing import Dict, List, Iterable
import os


def write_jsonl(file: str, data: Iterable[Dict], mode="wb"):
    def process_line(d: Dict):
        line = json.dumps(d, skipkeys=True, ensure_ascii=False)
        line = line + "\n"
        if "b" in mode:
            line = line.encode("utf-8")
        return line

    with gzip.open(file, mode=mode) if file.endswith("gz") else open(
        file, mode=mode
    ) as f:
        f.writelines((process_line(d) for d in data))


def write_json(file: str, datum: Dict, mode="wb"):
    with gzip.open(file, mode=mode) if file.endswith("gz") else open(
        file, mode=mode
    ) as f:
        line = json.dumps(datum, skipkeys=True, ensure_ascii=False)
        if "b" in mode:
            line = line.encode("utf-8")
        f.write(line)

def write_file(file, s: str, mode="wb"):
    with gzip.open(file, mode=mode) if file.endswith(".gz") else open(
        file, mode=mode
    ) as f:
        f.write(s.encode("utf-8"))


def write_lines(file, lines: Iterable[str], mode="wb"):
    def process_line(line):
        line = line + "\n"
        return line.encode("utf-8")

    with gzip.open(file, mode=mode) if file.endswith(".gz") else open(
        file, mode=mode
    ) as f:
        f.writelines((process_line(l) for l in lines))


def read_lines_from_files(path: str, mode="b", encoding="utf-8", limit=None):
    g = (
        line
        for file in os.listdir(path)
        for line in read_lines(os.path.join(path, file), mode, encoding)
    )
    for c, line in enumerate(g):
        if limit and (c >= limit):
            break
        yield line


def read_lines(file, mode="b", encoding="utf-8", limit=None):
    assert any([mode == m for m in ["b", "t"]])
    counter = 0
    with gzip.open(file, mode="r" + mode) if file.endswith(".gz") else open(
        file, mode="r" + mode
    ) as f:
        for line in f:
            counter += 1
            if limit and (counter > limit):
                break
            if "b" in mode:
                line = line.decode(encoding)
            yield line.replace("\n", "")


def read_jsonl(file, mode="b", limit=None, num_to_skip=0):
    assert any([mode == m for m in ["b", "t"]])
    with gzip.open(file, mode="r" + mode) if file.endswith(".gz") else open(
        file, mode="rb"
    ) as f:
        [next(f) for _ in range(num_to_skip)]
        for k, line in enumerate(f):
            if limit and (k >= limit):
                break
            yield json.loads(line.decode("utf-8") if mode == "b" else line)


def read_json(file: str, mode="b"):
    with gzip.open(file, mode="r" + mode) if file.endswith("gz") else open(
        file, mode="r" + mode
    ) as f:
        s = f.read()
        s = s.decode("utf-8") if mode == "b" else s
        return json.loads(s)


def download_data(base_url, file_name, data_folder, verbose=False, unzip_it=False,do_raise=True):
    if not os.path.exists(data_folder):
        os.makedirs(data_folder, exist_ok=True)

    url = base_url + "/" + file_name
    file = data_folder + "/" + file_name


    try:
        if unzip_it:
            if file_name.endswith(".zip"):
                extract_folder = file.replace('.zip', '')
                if not os.path.isdir(extract_folder):
                    wget_file(url, data_folder, verbose)
                    os.system('mkdir %s' % extract_folder)
                    os.system("unzip -d %s %s" % (extract_folder, file))
                    os.remove(file)
            elif file_name.endswith(".tar.gz"):
                extract_folder = file.replace('.tar.gz', '')
                if not os.path.isdir(extract_folder):
                    wget_file(url, data_folder, verbose)
                    os.system('mkdir %s' % extract_folder)
                    os.system("tar xzf %s -C %s" % (file, extract_folder))
                    os.remove(file)
        else:
            if not os.path.isfile(file):
                wget_file(url, data_folder, verbose)
    except FileNotFoundError as e:
        if do_raise:
            raise e

def wget_file(url, data_folder, verbose):
    err_code = os.system(
        "wget -c -N%s -P %s %s" % (" -q" if not verbose else "", data_folder, url))
    if err_code!=0:
        raise FileNotFoundError("could not downloaded %s" % url.split('/')[-1])