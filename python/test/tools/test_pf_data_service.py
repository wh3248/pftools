import unittest
from parflow.tools.pf_data_service import PFDataService
from pathlib import Path


class TestPFDataService(unittest.TestCase):
    def test_open_xarray(self):
        # Read a .pfb file as an xarray
        service = PFDataService()
        file_name = Path("../test/input/van-genuchten-alpha.pfb").resolve()
        ds = service.read_pfb_xarray_dataset(file_name)

        # Check expected dims and data values of loaded xarray
        self.assertIsNotNone(ds)
        self.assertEqual(10, ds.dims["x"])
        self.assertEqual(10, ds.dims["y"])
        self.assertEqual(8, ds.dims["z"])
        data = ds.to_array()
        self.assertEqual((1, 8, 10, 10), data.shape)
        self.assertEqual(4.0, data.sum().round())
        print(service.implementation_type)

    def test_read_pfb_numpy(self):
        """Test reading pfb file with header."""
        file_name = Path("../test/input/van-genuchten-alpha.pfb").resolve()
        service = PFDataService()
        header = {}
        # Call the service to read the data into a numpy array
        data = service.read_pfb_numpy(file_name, header)
        # check expected values of header, the shape of the numpy array and the sum of data
        self.assertIsNotNone(data)
        self.assertEqual(0, header.get("x"))
        self.assertEqual(0, header.get("y"))
        self.assertEqual(0, header.get("z"))
        self.assertEqual(10, header.get("nx"))
        self.assertEqual(10, header.get("ny"))
        self.assertEqual(8, header.get("nz"))
        self.assertEqual(0, header.get("dx"))
        self.assertEqual(0, header.get("dy"))
        self.assertEqual(0, header.get("dz"))
        self.assertEqual(1, header.get("p"))
        self.assertEqual(1, header.get("q"))
        self.assertEqual(1, header.get("r"))
        self.assertEqual(1, header.get("n_subgrids"))
        self.assertEqual((8, 10, 10), data.shape)
        self.assertEqual(4.0, data.sum().round())

    def test_read_pfb_files(self):
        """Test reading list of pfb files."""
        file_names = []
        file_names.append(Path("../test/input/van-genuchten-alpha.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-n.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-sr.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-ssat.pfb").resolve())
        service = PFDataService()
        data = service.read_pfb_files_numpy(file_names)
        self.assertEqual((4, 8, 10, 10), data.shape)

    def test_read_pfb_files_z_time(self):
        """Test reading list of numpy files when z is time."""
        file_names = []
        file_names.append(Path("../test/input/van-genuchten-alpha.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-n.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-sr.pfb").resolve())
        file_names.append(Path("../test/input/van-genuchten-ssat.pfb").resolve())
        service = PFDataService()
        data = service.read_pfb_files_numpy(file_names, z_is='time')
        self.assertEqual((32, 10, 10), data.shape)

    def test_read_pfb_numpy_no_header(self):
        """Test reading pfb files without reading header."""
        file_name = Path("../test/input/van-genuchten-alpha.pfb").resolve()
        service = PFDataService()
        # Call the service to read the data into a numpy array
        data = service.read_pfb_numpy(file_name)
        # check the values of the header, the shape of the numpy array and the sum of data
        self.assertEqual((8, 10, 10), data.shape)


if __name__ == "__main__":
    unittest.main()
