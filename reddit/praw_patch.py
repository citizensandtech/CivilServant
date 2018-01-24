
from pathlib import Path
import argparse
import hashlib
import os
import praw
import subprocess
import tempfile

from utils.common import BASE_DIR

PRAW_LIB_DIR = str(Path(praw.__file__).parent)
PRAW_DIFFS_DIR = str(Path(BASE_DIR, "reddit", "praw_patches"))
Path(PRAW_DIFFS_DIR).mkdir(parents=True, exist_ok=True)

class PrawPatch:
    def __init__(self, version=praw.__version__):
        self.version = version
        self.path = Path(PRAW_DIFFS_DIR, self.version)
        
        self.diffs = list(self.path.rglob("*.diff"))
        if not self.diffs:
            raise PrawPatchException("no patch created for praw " + self.version)
        
        self.diffs_unapplied = [d for d in self.diffs if not self._diff_applied(d)]

    def __repr__(self):
        return "<PrawPatch version=%s applied=%s>" % (self.version, str(self.applied))

    def _apply_diff(self, diff_path):
        with tempfile.NamedTemporaryFile("r") as tmpfile:
            source_path = Path(PRAW_LIB_DIR, diff_path.stem)
            target_path = Path(tmpfile.name)
            
            with open(os.devnull, "w") as devnull:
                args = ["patch", str(source_path), str(diff_path), "-o", str(target_path)]
                subprocess.run(args, check=True, stdout=devnull)

            if not self._diff_applied(diff_path, source_path=target_path):
                raise PrawPatchException("md5 digest mismatch: " + str(diff_path))

            with open(str(source_path), "wb") as f:
                f.write(target_path.read_text().encode("utf8"))

    def _diff_applied(self, diff_path, source_path=None):
        if not source_path:
            source_path = Path(PRAW_LIB_DIR, diff_path.stem)
        source = source_path.read_text().encode("ascii")

        source_digest = hashlib.md5(source).hexdigest()
        target_digest = diff_path.with_name(diff_path.stem + ".md5").read_text()[:-1]
        
        return source_digest == target_digest

    def apply(self):
        for diff_path in self.diffs_unapplied:
            self._apply_diff(diff_path)
        self.diffs_unapplied = []
    
    @property
    def applied(self):
        return not bool(self.diffs_unapplied)

    def ensure_applied(self):
        if not self.applied:
            path_strs = str([str(d) for d in self.diffs_unapplied])[1:-1]
            raise PrawPatchException("praw diffs not applied: " + path_strs)
    
    @property
    def required(self):
        return bool(int(os.environ.get("CS_PRAW_DIFFS_REQUIRED", True)))

class PrawPatchException(Exception):
    pass

def parse_args():
    parser = argparse.ArgumentParser()
    parser.add_argument("-v", "--version", required=False, type=str,
                        default=praw.__version__,
                        help="Specify the praw version.")

    mutex = parser.add_mutually_exclusive_group(required=True)
    mutex.add_argument("-a", "--apply", required=False, action="store_true",
                       help="Apply the praw patch for the current version.")
    mutex.add_argument("-r", "--required", required=False, action="store_true",
                       help="Check if praw patches are required by the current environment.")
    mutex.add_argument("-u", "--unapplied", required=False, action="store_true",
                       help="Get the paths of any unapplied diffs for the current version.")

    args = parser.parse_args()
    return args

if __name__ == '__main__':
    args = parse_args()
    patch = PrawPatch(args.version)
    if args.apply:
        patch.apply()
    elif args.required:
        print(patch.required)
    elif args.unapplied:
        print([str(d) for d in patch.diffs_unapplied])
    
