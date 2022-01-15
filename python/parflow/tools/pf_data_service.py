import xarray as xr
import os

class PFDataService:
    """
    Stateless service for reading and writing perflow .pfb binary data files.
    This supports reading into xarray and also reading into numpy ndarrays.
    The class is stateless except for initial configuration of options that
    can be configured by the application before using the service.
    """

    def __init__(self):
        # Dependency injected configuration options
        self.z_first = True
        self.z_is = "z"
        self.parflow_binary_reader_class = None
        self.parflow_backend_entrypoint_class = None
        self.read_stack_of_pfbs = None
        self.implementation_type = None

        # Configure defaults for dependency injected options
        self.configure_dependency_injection()

    def read_pfb_xarray_dataset(self, file_name, header = None):
        """
        Read a parflow .pfb file(s) using xarray and return an xarray dataset.
        The file_name can be a a single .pfb file or a parflow .json file of metadata.
        If the file is a .json file of meta data then all the .pfb files in the meta data
        are loaded into the xarray dataset.

        If the header parameter is specified and is a dict object this dict is populated with
        the header information about the pfb file.(s) The header is populated with keys:
          x, y, z, nx, ny, nz, dx, dy, dz, p, q, r, n_subgrids
        """

        result = None
        ds = xr.open_dataset(str(file_name), name='xxdefault_single', engine=self.parflow_backend_entrypoint_class)

        return ds

    def read_pfb_xarray_dataarray(self, file_name):
        """
        Read a single parflow .pfb file(s) using xarray and return an xarray dataarray.
        """

        result = None
        name = os.path.basename(file_name)
        da = xr.open_dataarray(str(file_name), name=name, engine=self.parflow_backend_entrypoint_class)

        return da

    def read_pfb_numpy(self, file_name, header = None, mode='full'):
        """
        Read a parflow .pfb binary file.
        The method returns a numpy ndarray object with the data in the pfb file.
        The data depends on the mode.
            mode='full'  Returns dimensions (nx, ny, nz)
            mode='flat'  Returns dimensions 
            mode='tiled' Returns dimensions (p, q, r)

        If the header is specified and is a dict object this dict is populated with
        the header information about the pfb file. The headers is populated with keys:
          x, y, z, nx, ny, nz, dx, dy, dz, p, q, r, n_subgrids
        """

        with self.parflow_binary_reader_class(file_name) as pfb:
            data = pfb.read_all_subgrids(mode=mode, z_first=self.z_first)
            if header is not None:
                header['x'] = pfb.header.get('x', None)
                header['y'] = pfb.header.get('y', None)
                header['z'] = pfb.header.get('z', None)
                header['dx'] = pfb.header.get('dx', None)
                header['dy'] = pfb.header.get('dy', None)
                header['dz'] = pfb.header.get('dz', None)
                header['nx'] = pfb.header.get('nx', None)
                header['ny'] = pfb.header.get('ny', None)
                header['nz'] = pfb.header.get('nz', None)
                header['p'] = pfb.header.get('p', None)
                header['q'] = pfb.header.get('q', None)
                header['r'] = pfb.header.get('r', None)
                header['n_subgrids'] = pfb.header.get('n_subgrids', None)
        return data

    def read_pfb_files_numpy(self, file_names, header = None, mode='full', z_is=None):
        """
        Read a list of parflow .pfb binary files.
        The file_names is a list of file names to be loaded.
        The method returns a numpy ngarray object with the data in the pfb files.
        The data depends on the mode.
            mode='full'  Returns dimensions (nx, ny, nz)
            mode='flat'  Returns dimensions 
            mode='tiled' Returns dimensions (p, q, r)

        If the header is specified and is a dict object this dict is populated with
        the header information about the pfb files. The headers is populated with keys:
          x, y, z, nx, ny, nz, dx, dy, dz, p, q, r, n_subgrids
        """

        if z_is is None:
            z_is = self.z_is
        ng = self.read_stack_of_pfbs(file_names, z_first = self.z_first, z_is = z_is)
        return ng

    def read_pfb_header(self, file_name):
        """
        Read a parflow .pfb binary file, return a dict with the header values.
          The header contains keys: x, y, z, nx, ny, nz, dx, dy, dz, p, q, r, n_subgrids
        """
        header = None
        return header

    def write_dist_files_from_numpy(self, header, data, mode='full'):
        pass

    def write_pfb_from_numpy(self, header, data, mode='full'):
        pass

    def configure_dependency_injection(self):
        """
        Initialize the dependency injected methods and classes.
        There are two implementations. One implementation is portable that will work
        on python 3.8, 3.7 and does not use numba that does not always work in all
        environments.

        The other implementation is the default. It performs faster because it uses numba
        and it also uses some strong typing features of python 3.9.
        """

        try:
            # Try to configure using the implentation with python 3.9 and numba
            from parflow.tools.pf_xarray.io import ParflowBinaryReader
            from parflow.tools.pf_xarray.pf_backend import ParflowBackendEntrypoint
            from parflow.tools.pf_xarray.io import read_stack_of_pfbs

            self.parflow_binary_reader_class = ParflowBinaryReader
            self.parflow_backend_entrypoint_class = ParflowBackendEntrypoint
            self.read_stack_of_pfbs = read_stack_of_pfbs
            self.implementation_type = "default"
        except Exception as e:
            # If an error occurs then fall back to the portable implementation
            from parflow.tools.pf_xarray.portable.io import ParflowBinaryReader
            from parflow.tools.pf_xarray.portable.pf_backend import ParflowBackendEntrypoint
            from parflow.tools.pf_xarray.portable.io import read_stack_of_pfbs
            self.parflow_binary_reader_class = ParflowBinaryReader
            self.parflow_backend_entrypoint_class = ParflowBackendEntrypoint
            self.read_stack_of_pfbs = read_stack_of_pfbs
            self.implementation_type = "portable"