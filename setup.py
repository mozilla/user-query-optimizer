import codecs
import os
import re

from setuptools import setup, find_packages

###################################################################

NAME = "user-query-optimizer"
PACKAGES = find_packages(where="user_query_optimizer")
META_PATH = os.path.join("user_query_optimizer", "__init__.py")
CLASSIFIERS = [
    "Natural Language :: English",
    "License :: OSI Approved :: Mozilla Public License 2.0 (MPL 2.0)",
    "Operating System :: OS Independent",
    "Programming Language :: Python",
    "Programming Language :: Python :: 3",
    "Programming Language :: Python :: 3.3",
    "Programming Language :: Python :: 3.4",
    "Programming Language :: Python :: 3.5",
    "Programming Language :: Python :: 3.6",
    "Programming Language :: Python :: Implementation :: PyPy",
    "Topic :: Software Development :: Libraries :: Python Modules",
]
INSTALL_REQUIRES = []

###################################################################

HERE = os.path.abspath(os.path.dirname(__file__))


def read(*parts):
    """
    Build an absolute path from *parts* and and return the contents of the
    resulting file.  Assume UTF-8 encoding.
    """
    with codecs.open(os.path.join(HERE, *parts), "rb", "utf-8") as f:
        return f.read()


META_FILE = read(META_PATH)


def find_meta(meta):
    """
    Extract __*meta*__ from META_FILE.
    """
    meta_match = re.search(
        r"^__{meta}__ = ['\"]([^'\"]*)['\"]".format(meta=meta),
        META_FILE, re.M
    )
    if meta_match:
        return meta_match.group(1)
    raise RuntimeError("Unable to find __{meta}__ string.".format(meta=meta))


if __name__ == "__main__":
    setup(
        name=NAME,
        description=find_meta("description"),
        license=find_meta("license"),
        url=find_meta("uri"),
        version=find_meta("version"),
        author=find_meta("author"),
        author_email=find_meta("email"),
        maintainer=find_meta("author"),
        maintainer_email=find_meta("email"),
        long_description=read("README.md"),
        packages=PACKAGES,
        package_dir={"": "user_query_optimizer"},
        zip_safe=False,
        classifiers=CLASSIFIERS,
        install_requires=INSTALL_REQUIRES,
    )

# setup(name='user-query-optimizer',
#       version='0.1',
#       description='SQL query optimizer',
#       url='https://github.com/swathiiyer2/user-query-optimizer',
#       author='Swathi Iyer',
#       author_email='swathii@stanford.edu',
#       packages=['user-query-optimizer'],
#       zip_safe=False)
