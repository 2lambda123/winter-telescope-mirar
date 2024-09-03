"""
Module with pipeline for building reference images in the IR from WFAU
"""
import logging

from mirar.data import Image
from mirar.downloader.caltech import download_via_ssh
from mirar.io import open_mef_image
from mirar.pipelines.base_pipeline import Pipeline
from mirar.pipelines.winter.blocks import astrometry
from mirar.pipelines.winter.blocks import avro_broadcast
from mirar.pipelines.winter.blocks import avro_export
from mirar.pipelines.winter.blocks import build_test
from mirar.pipelines.winter.blocks import csvlog
from mirar.pipelines.winter.blocks import detect_candidates
from mirar.pipelines.winter.blocks import detrend_unpacked
from mirar.pipelines.winter.blocks import diff_forced_photometry
from mirar.pipelines.winter.blocks import extract_all
from mirar.pipelines.winter.blocks import focus_cals
from mirar.pipelines.winter.blocks import full_reduction
from mirar.pipelines.winter.blocks import imsub
from mirar.pipelines.winter.blocks import load_avro
from mirar.pipelines.winter.blocks import load_calibrated
from mirar.pipelines.winter.blocks import load_final_stack
from mirar.pipelines.winter.blocks import load_raw
from mirar.pipelines.winter.blocks import load_skyportal
from mirar.pipelines.winter.blocks import load_sources
from mirar.pipelines.winter.blocks import load_test
from mirar.pipelines.winter.blocks import mask_and_split
from mirar.pipelines.winter.blocks import mosaic
from mirar.pipelines.winter.blocks import only_ref
from mirar.pipelines.winter.blocks import photcal_stacks
from mirar.pipelines.winter.blocks import plot_stack
from mirar.pipelines.winter.blocks import process_candidates
from mirar.pipelines.winter.blocks import realtime
from mirar.pipelines.winter.blocks import reduce
from mirar.pipelines.winter.blocks import reduce_unpacked
from mirar.pipelines.winter.blocks import reduce_unpacked_subset
from mirar.pipelines.winter.blocks import refbuild
from mirar.pipelines.winter.blocks import reftest
from mirar.pipelines.winter.blocks import remask
from mirar.pipelines.winter.blocks import save_raw
from mirar.pipelines.winter.blocks import select_split_subset
from mirar.pipelines.winter.blocks import send_to_skyportal
from mirar.pipelines.winter.blocks import stack_forced_photometry
from mirar.pipelines.winter.blocks import stack_stacks
from mirar.pipelines.winter.blocks import unpack_all
from mirar.pipelines.winter.blocks import unpack_subset
from mirar.pipelines.winter.config import PIPELINE_NAME
from mirar.pipelines.winter.config import winter_cal_requirements
from mirar.pipelines.winter.load_winter_image import load_raw_winter_mef
from mirar.pipelines.winter.models import set_up_winter_databases

logger = logging.getLogger(__name__)


class WINTERPipeline(Pipeline):
    """Pipeline for processing WINTER data"""

    name = "winter"
    default_cal_requirements = winter_cal_requirements

    all_pipeline_configurations = {
        "astrometry": load_calibrated + astrometry,
        "unpack_subset": unpack_subset,
        "unpack_all": unpack_all,
        "detrend_unpacked": detrend_unpacked,
        "imsub": load_final_stack + imsub,
        "reduce": reduce,
        "reduce_unpacked": reduce_unpacked,
        "photcal_stacks": photcal_stacks,
        "plot_stacks": load_final_stack + plot_stack,
        "buildtest": build_test,
        "test": load_test
        + csvlog
        + extract_all
        + mask_and_split
        + select_split_subset
        + save_raw
        + full_reduction
        + imsub
        + detect_candidates
        + process_candidates,
        "refbuild": refbuild,
        "reftest": reftest,
        "only_ref": only_ref,
        "realtime": realtime,
        "detect_candidates": load_final_stack
        + imsub
        + detect_candidates
        + process_candidates
        + avro_broadcast,
        "recandidates": load_sources + process_candidates + avro_broadcast,
        "default": reduce
        + imsub
        + detect_candidates
        + process_candidates
        + avro_broadcast,
        "remask": remask,
        "default_subset": reduce_unpacked_subset
        + imsub
        + detect_candidates
        + process_candidates
        + avro_broadcast,
        "stack_stacks": load_final_stack + stack_stacks,
        "stack_stacks_db": stack_stacks,
        "focus_cals": focus_cals,
        "mosaic": mosaic,
        "log": load_raw + extract_all + csvlog,
        "skyportal": load_skyportal + send_to_skyportal,
        "diff_forced_phot": diff_forced_photometry,
        "stack_forced_phot": stack_forced_photometry,
        "rebroadcast_avro": load_avro + avro_export,
    }

    non_linear_level = 40000.0

    @staticmethod
    def _load_raw_image(path: str) -> Image | list[Image]:
        """

        :param path: str:

        """
        return open_mef_image(path, load_raw_winter_mef, extension_key="BOARD_ID")

    @staticmethod
    def download_raw_images_for_night(night: str):
        """

        :param night: str:

        """
        download_via_ssh(
            server="winter.caltech.edu",
            base_dir="/data/loki/raw_data/winter",
            night=night,
            pipeline=PIPELINE_NAME,
            server_sub_dir="raw",
        )

    def set_up_pipeline(self):
        """ """
        set_up_winter_databases()
