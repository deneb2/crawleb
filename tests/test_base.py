import os
import sys
import json
import shutil
import unittest

from StringIO import StringIO
from contextlib import contextmanager


def ordered(obj):
    if isinstance(obj, str):
        try:
            obj = json.loads(obj)
        except:
            try:
                obj = eval(obj)
            except:
                pass
    if isinstance(obj, dict):
        return sorted((k, ordered(v)) for k, v in obj.items())
    if isinstance(obj, list):
        return sorted(ordered(x) for x in obj)
    else:
        return obj

class BaseTestClass(unittest.TestCase):
    _temp_fd = None
    
    def setUp(self):
        # create a new temporary directory
        try:
            shutil.rmtree(self._tmp_path)
        except:
            pass
        try:
            os.makedirs(self._tmp_path)
        except Exception, e:
            sys.stderr.write("Impossible to create tmp directory %s\n" %(e,))
            sys.exit(1)

    @property
    def _test_base_dir(self):
        return os.path.dirname(
            sys.modules[self.__class__.__module__]
            .__file__
        )
    
    @property
    def _test_name(self):
        return self._testMethodName[5:]

    @property
    def input_data_file(self):
        return self._test_base_dir + "/data/" + self._test_name + "_input"

    @property
    def _output_data_file(self):
        return self._test_base_dir + "/data/" + self._test_name + "_output"

    @property
    def _tmp_path(self):
        return '/tmp/'+ self._test_base_dir + '/data/' + self._test_name + "/"

    @property
    def tmp_file(self):
        return self._tmp_path + self._test_name + "_current"

    def _open_temporary(self):
        self._temp_fd = open(self.tmp_file, "a")

    def _read_file_by_lines(self, fd):
        input = []
        for i in fd:
            line = i.strip()
            input.append(line)
        return input

    def input_data(self):
        fd = open(self.input_data_file, "r")
        return self._read_file_by_lines(fd)

    def output_data(self):
        fd = open(self._output_data_file, "r")
        return self._read_file_by_lines(fd)

    def temporary_data(self):
        fd = open(self.tmp_file, "r")
        return self._read_file_by_lines(fd)

    def store_temporary(self, string):
        if not self._temp_fd:
            self._open_temporary()
        self._temp_fd.write("%s\n" %(string,))

    def assertEqualTemporary(self):
        try:
            self._temp_fd.close()
        except:
            pass
        output = self.output_data()
        temporary = self.temporary_data()
        for i, o in enumerate(output):
            self.assertEqual(ordered(o), ordered(temporary[i]))

    @contextmanager
    def captured_output(self):
        new_out, new_err = StringIO(), StringIO()
        old_out, old_err = sys.stdout, sys.stderr
        try:
            sys.stdout, sys.stderr = new_out, new_err
            yield sys.stdout, sys.stderr
        finally:
            sys.stdout, sys.stderr = old_out, old_err
