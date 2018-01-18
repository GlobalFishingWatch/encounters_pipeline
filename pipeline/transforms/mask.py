# from skimage import io
# import numpy as np
# imarray = io.imread("dist2coast_1deg_ocean_negative-land_v3.tiff")
# np.save("inland.npy", imarray == -10)
# sparsify("../inland.npy", "sparse_inland.pickle")

from __future__ import print_function, division
import pickle
import time
import bisect
import array




class BaseMask(object):

    @staticmethod
    def _load_mask(path, threshold, invert):
        import rasterio
        with rasterio.open(path) as src:
            [img] = src.read()
            height, width = img.shape
        [first_x, dx, shear_0, first_y, shear_1, dy] = src.transform
        assert shear_0 == shear_1 == 0, "shearing is not supported"
        assert dx > 0 and dy < 0, "only normal image orientation supported"
        last_x = first_x + dx * width
        last_y = first_y + dy * height

        min_x = min(first_x, last_x)
        max_x = max(first_x, last_x)
        min_y = min(first_y, last_y)
        max_y = max(first_y, last_y)

        mask = (img > threshold)
        if invert:
            mask = ~mask

        return (min_x, max_x, min_y, max_y), mask

    @classmethod
    def sparsify(cls, inpath, outpath, threshold=0.5, invert=False):
        import numpy as np
        (min_lon, max_lon, min_lat, max_lat), dense = cls._load_mask(inpath, threshold, invert)
        sparse = []
        indices = np.arange(dense.shape[1])
        assert dense.shape[1] <= 65535
        for i, row in enumerate(dense):
            # Ensure boolean and copy
            mask = (row != 0) 
            # First element of mask is just row[0],
            # subsequent elements are true if that element
            # of row differs from the element previous.
            mask[1:] ^= row[:-1]
            # For each true element of mask store an index
            # as an unsigned short.
            sparse.append(array.array('H', indices[mask]))
        mask_info = {
            'min_lon': min_lon,
            'max_lon': max_lon,
            'min_lat': min_lat,
            'max_lat': max_lat,
            'n_lat': dense.shape[0],
            'n_lon': dense.shape[1],
            'data': tuple(sparse)
        }
        with open(outpath, "wb") as f:
            pickle.dump(mask_info, f)






class Mask(BaseMask):

    def __init__(self, path, check=False):
        with open(path) as f:
            mask_info = pickle.load(f)
        self.mask_data = mask_info['data']
        self.MAX_LAT = mask_info['max_lat']
        self.MIN_LAT = mask_info['min_lat']
        self.MAX_LON = mask_info['max_lon']
        self.MIN_LON = mask_info['min_lon']
        self._dlat = (self.MAX_LAT - self.MIN_LAT) / mask_info['n_lat']
        self._dlon = (self.MAX_LON - self.MIN_LON) / mask_info['n_lon']
        self.check = check

    def query(self, lat, lon):
        if self.check:
            assert self.MIN_LAT <= lat < self.MAX_LAT
            assert self.MIN_LON <= lat < self.MAX_LON
        i = (self.MAX_LAT - lat) // self._dlat
        j = (lon - self.MIN_LON) // self._dlon
        ndx = bisect.bisect_right(self.mask_data[int(i)], j)
        return ndx & 1




class SimpleMask(BaseMask):

    def __init__(self, path, threshold, invert):
        import numpy as np
        bounds, self.mask = self._load_mask(path, threshold, invert)
        self.MIN_LON, self.MAX_LON, self.MIN_LAT, self.MAX_LAT = bounds
        self.nlat, self.nlon = self.mask.shape
        self.dlat = (self.MAX_LAT - self.MIN_LAT) / self.nlat
        self.dlon = (self.MAX_LON - self.MIN_LON) / self.nlon

    def query(self, loc):
        lat, lon = loc
        i = int((self.MAX_LAT - lat) // self.dlat)
        j = int((lon - self.MIN_LON) // self.dlon)
        return self.mask[i, j]


