from distutils.core import setup
from distutils.command.install_data import install_data
from distutils.command.install import INSTALL_SCHEMES
import os
import sys


class osx_install_data(install_data):
    # On MacOS, the platform-specific lib dir is /System/Library/Framework/Python/.../
    # which is wrong. Python 2.5 supplied with MacOS 10.5 has an Apple-specific fix
    # for this in distutils.command.install_data#306. It fixes install_lib but not
    # install_data, which is why we roll our own install_data class.

    def finalize_options(self):
        # By the time finalize_options is called, install.install_lib is set to the
        # fixed directory, so we set the installdir to install_lib. The
        # install_data class uses ('install_data', 'install_dir') instead.
        self.set_undefined_options('install', ('install_lib', 'install_dir'))
        install_data.finalize_options(self)

if sys.platform == "darwin":
    cmdclasses = {'install_data': osx_install_data}
else:
    cmdclasses = {'install_data': install_data}


def fullsplit(path, result=None):
    """
    Split a pathname into components (the opposite of os.path.join) in a
    platform-neutral way.
    """
    if result is None:
        result = []
    head, tail = os.path.split(path)
    if head == '':
        return [tail] + result
    if head == path:
        return result
    return fullsplit(head, [tail] + result)

# Tell distutils to put the data_files in platform-specific installation
# locations. See here for an explanation:
# http://groups.google.com/group/comp.lang.python/browse_thread/thread/35ec7b2fed36eaec/2105ee4d9e8042cb
for scheme in INSTALL_SCHEMES.values():
    scheme['data'] = scheme['purelib']

# Compile the list of packages available, because distutils doesn't have
# an easy way to do this.
packages, data_files = [], []
root_dir = os.path.dirname(__file__)
if root_dir != '':
    os.chdir(root_dir)
jdt_dir = 'jdt'

for dirpath, dirnames, filenames in os.walk(jdt_dir):
    # Ignore dirnames that start with '.'
    for i, dirname in enumerate(dirnames):
        if dirname.startswith('.'):
            del dirnames[i]
    if '__init__.py' in filenames:
        packages.append('.'.join(fullsplit(dirpath)))
    elif filenames:
        data_files.append([dirpath, [os.path.join(dirpath, f)
                                     for f in filenames]])

# Small hack for working with bdist_wininst.
# See http://mail.python.org/pipermail/distutils-sig/2004-August/004134.html
if len(sys.argv) > 1 and sys.argv[1] == 'bdist_wininst':
    for file_info in data_files:
        file_info[0] = '\\PURELIB\\%s' % file_info[0]


setup(
    name="jdt",
    version="0.5.7",
    description="JSON Data Tools",
    long_description="""A collection of scripts for working with CSV, JSON, and NDJSON and importing into MongoDB and FHIR.""",
    author="Alan Viars",
    author_email="sales@videntity.com",
    url="https://github.com/videntity/json-data-tools",
    download_url="https://gitbub.com/videntity/json-data-tools/tarball/master",
    install_requires=[
        'pymongo', 'ndjson', 'requests', 'pysftp',
    ],
    packages=packages,
    scripts=[
        'jdt/csv2mongo',
        'jdt/json2mongo',
        'jdt/jsondir2ndjson',
        'jdt/ndjson2mongo',
        'jdt/ndjsonurl2mongo',
        'jdt/ndjson2fhir',
        'jdt/ndjsonurl2fhir',
        'jdt/sftp_ndjson2mongo',
        'jdt/fhirbundles2mongo',
        'jdt/jsonlist2ndjson',
        'jdt/split_large_files',
        'jdt/fhirbundle2ndjson',
        'jdt/fhirbundledownsize',
        'jdt/fhirbundle2mongo',
    ])
