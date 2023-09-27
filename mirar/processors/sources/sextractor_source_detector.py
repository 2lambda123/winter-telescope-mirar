"""
Module to detect candidates in an image
"""

import logging
from pathlib import Path

import numpy as np
import pandas as pd

from mirar.data import Image, ImageBatch, SourceBatch, SourceTable
from mirar.paths import (
    BASE_NAME_KEY,
    CAND_DEC_KEY,
    CAND_RA_KEY,
    SEXTRACTOR_HEADER_KEY,
    XPOS_KEY,
    YPOS_KEY,
    get_output_dir,
)
from mirar.processors.astromatic import Sextractor
from mirar.processors.base_processor import BaseSourceGenerator, PrerequisiteError
from mirar.utils.ldac_tools import get_table_from_ldac

logger = logging.getLogger(__name__)


def generate_candidates_table(
    image: Image,
    sex_catalog_path: str | Path,
) -> pd.DataFrame:
    """
    Generate a candidates table from a sextractor catalog
    :param sex_catalog_path: Path to the sextractor catalog
    :return: Candidates table
    """
    det_srcs = get_table_from_ldac(sex_catalog_path)

    multi_col_mask = [det_srcs.dtype[i].shape != () for i in range(len(det_srcs.dtype))]
    if np.sum(multi_col_mask) != 0:
        logger.warning(
            "Sextractor catalog contains multi-dimensional columns, "
            "removing them before converting to pandas dataframe"
        )
        to_remove = np.array(det_srcs.colnames)[multi_col_mask]
        det_srcs.remove_columns(to_remove)

    det_srcs = det_srcs.to_pandas()
    logger.debug(f"Found {len(det_srcs)} sources in image.")

    ydims, xdims = image.get_data().shape
    det_srcs["NAXIS1"] = xdims
    det_srcs["NAXIS2"] = ydims
    det_srcs[XPOS_KEY] = det_srcs["X_IMAGE"] - 1
    det_srcs[YPOS_KEY] = det_srcs["Y_IMAGE"] - 1
    det_srcs[CAND_RA_KEY] = det_srcs["ALPHAWIN_J2000"]
    det_srcs[CAND_DEC_KEY] = det_srcs["DELTAWIN_J2000"]
    det_srcs["fwhm"] = det_srcs["FWHM_IMAGE"]
    det_srcs["elong"] = det_srcs["ELONGATION"]

    return det_srcs


class SextractorSourceDetector(BaseSourceGenerator):
    """
    Class that retrieves a sextractor catalog and saves all sources to a sourcetable
    """

    base_key = "DETSOURC"

    def __init__(
        self,
        output_sub_dir: str = "sources",
    ):
        super().__init__()
        self.output_sub_dir = output_sub_dir

    def __str__(self) -> str:
        return "Retrieves a sextractor catalog and converts it to a sourcetable"

    def get_sub_output_dir(self) -> Path:
        """
        Returns: output sub-directory
        """
        return get_output_dir(self.output_sub_dir, self.night_sub_dir)

    def _apply_to_images(
        self,
        batch: ImageBatch,
    ) -> SourceBatch:
        all_sources = SourceBatch()
        for image in batch:
            srcs_table = generate_candidates_table(
                image=image,
                sex_catalog_path=image[SEXTRACTOR_HEADER_KEY],
            )

            if len(srcs_table) > 0:
                x_shape, y_shape = image.get_data().shape
                srcs_table["X_SHAPE"] = x_shape
                srcs_table["Y_SHAPE"] = y_shape

            metadata = self.get_metadata(image)

            if len(srcs_table) == 0:
                msg = f"No sources found in image {image[BASE_NAME_KEY]}"
                logger.warning(msg)

            else:
                msg = f"Found {len(srcs_table)} sources in image {image[BASE_NAME_KEY]}"
                logger.debug(msg)
                all_sources.append(SourceTable(srcs_table, metadata=metadata))

        return all_sources

    def check_prerequisites(self):
        check = np.sum([isinstance(x, Sextractor) for x in self.preceding_steps])
        if check == 0:
            raise PrerequisiteError(
                "Sextractor must be run before SextractorSourceDetector"
            )