import string
import warnings

import cmweather
import numpy as np
import pandas as pd
import xarray as xr
import matplotlib.colors as mcolors
import matplotlib.pyplot as plt

from typing import TypedDict, List
from xarray.core.dataset import Dataset


class MomentSpec(TypedDict):
    cmap: str | None
    vmin: float | None
    vmax: float | None
    norm: str | None
    label: str | None
    mask_below: float | None
    classes: dict[int, str] | None


# Colour of each classification label, so a class keeps its colour whatever value a product assigns to it.
CLASS_COLOURS: dict[str, str] = {
    # Rainfields echo classification (CLASS): precip / clear air / clutter.
    "conv": "#c51b1b", "sconv": "#f28e2b", "strat": "#4daf4a",
    "insect": "#e6c229", "smoke": "#b39b82",
    "chaff": "#17becf", "bird": "#8c564b", "cx_gnd": "#404040", "ap_gnd": "#7f7f7f", "ap_sea": "#6baed6",
    "cx_sea": "#2171b5", "2trip": "#9467bd", "eemit": "#e377c2", "speck": "#bdbdbd", "blip": "#000000",
    # Rainfields hydrometeor classification (HCLASS): liquid / hail-graupel / ice / non-meteorological.
    "cl": "#e0e0e0",
    "drz": "#c7e9c0", "lr": "#74c476", "mr": "#31a354", "hr": "#006d2c",
    "ha": "#d62728", "rh": "#ff7f0e", "gsh": "#e377c2", "grr": "#fdb863",
    "ds": "#c6dbef", "ws": "#4292c6", "ic": "#bcbddc", "iic": "#756bb1", "sld": "#08306b",
    "bgs": "#8c564b", "trip2": "#bcbd22", "gcl": "#525252", "misc": "#969696",
}
# Fallback class tables, for files without flag_values/flag_meanings attributes on the classification field.
ECHO_CLASSES: dict[int, str] = dict(enumerate(
    "conv,sconv,strat,insect,smoke,chaff,bird,cx_gnd,ap_gnd,ap_sea,cx_sea,2trip,eemit,speck,blip".split(","), start=1
))
HYDRO_CLASSES: dict[int, str] = dict(zip(
    (1, 2, 3, 4, 5, 6, 7, 8, 9, 10, 11, 12, 13, 14, 15, 16, 17, 19, 20),
    "cl,drz,lr,mr,hr,ha,rh,gsh,grr,ds,ws,ic,iic,sld,bgs,trip2,gcl,chaff,misc".split(","),
))

MOMENT_SPECS: dict[str, MomentSpec] = {
    "DBZH": MomentSpec(cmap="HomeyerRainbow", vmin=-10, vmax=65, norm="linear", label="Reflectivity (dBZ)"),
    "DBZH_CLEAN": MomentSpec(cmap="HomeyerRainbow", vmin=-10, vmax=65, norm="linear", label="Reflectivity (dBZ)", mask_below=-31.0),
    "ZDR": MomentSpec(cmap="RefDiff", vmin=-2, vmax=8, norm="linear", label="Z$_{DR}$ (dB)"),
    "RHOHV": MomentSpec(cmap="RefDiff", vmin=0.5, vmax=1.05, norm="linear", label=r"$\rho_{HV}$"),
    "KDP": MomentSpec(cmap="Theodore16", vmin=-1, vmax=5, norm="linear", label="K$_{DP}$ (deg/km)"),
    "PHIDP": MomentSpec(cmap="Wild25", vmin=0, vmax=360, norm="linear", label=r"$\Phi_{DP}$ (deg)"),
    "VRADH": MomentSpec(cmap="RdBu_r", vmin=None, vmax=None, norm="diverging", label="Radial velocity (m/s)"),
    "WRADH": MomentSpec(cmap="viridis", vmin=0, vmax=8, norm="linear", label="Spectrum width (m/s)"),
    "SNRH": MomentSpec(cmap="Carbone17", vmin=-20, vmax=30, norm="linear", label="SNR (dB)"),
    "SQIH": MomentSpec(cmap="Carbone17", vmin=0, vmax=1, norm="linear", label="SQI"),
    "TEMPERATURE": MomentSpec(cmap="coolwarm", vmin=-60, vmax=30, norm="diverging", label="Temperature (degC)"),
    "CLASS": MomentSpec(cmap=None, vmin=None, vmax=None, norm="categorical", label="Echo classification", classes=ECHO_CLASSES),
    "HCLASS": MomentSpec(cmap=None, vmin=None, vmax=None, norm="categorical", label="Hydrometeor classification", classes=HYDRO_CLASSES),
    "SDR": MomentSpec(cmap="nwsref", vmin=-40, vmax=10, norm="linear", label="SDR (dB)"),
    "TEXTURE_ZDR": MomentSpec(cmap="cividis", vmin=0, vmax=3, norm="linear", label=r"SD(Z$_{DR}$) (dB)"),
    "TEXTURE_PHIDP": MomentSpec(cmap="cividis", vmin=0, vmax=90, norm="linear", label=r"SD($\Phi_{DP}$) (deg)"),
    "TEXTURE_RHOHV": MomentSpec(cmap="cividis", vmin=0, vmax=0.3, norm="linear", label=r"SD($\rho_{HV}$)"),
    "CONTRAST_RHOHV": MomentSpec(cmap="cividis", vmin=0, vmax=40, norm="linear", label=r"GLCM contrast $\rho_{HV}$"),
    "CONTRAST_ZDR": MomentSpec(cmap="cividis", vmin=0, vmax=40, norm="linear", label=r"GLCM contrast Z$_{DR}$"),
    "POSTERIOR": MomentSpec(cmap="viridis", vmin=0, vmax=1, norm="linear", label="Posterior probability "),
}
DEFAULT_DP: List[str] = ["DBZH", "DBZH_CLEAN", "ZDR", "RHOHV", "KDP", "PHIDP", "VRADH", "SNRH", "CLASS"]
DEFAULT_DOP: List[str] = ["DBZH", "DBZH_CLEAN", "VRADH"]
DEFAULT_SP: List[str] = ["DBZH", "DBZH_CLEAN"]


def check_moments(radar: xr.Dataset, moments: List[str]) -> List[str]:
    """Check that all moments exist in the radar dataset and return them."""
    for moment in moments:
        if moment not in radar.data_vars:
            raise ValueError(f"Moment '{moment}' is not defined in the radar dataset.")
    return list(moments)


def get_spec(moment: str) -> MomentSpec:
    return MOMENT_SPECS.get(moment, MomentSpec(cmap=None, vmin=None, vmax=None, norm="linear", label=moment))


def get_classes(field: xr.DataArray, spec: MomentSpec) -> dict[int, str] | None:
    """Class values and labels of a categorical field, read from its CF flag attributes, else from its MomentSpec."""
    if "flag_values" in field.attrs and "flag_meanings" in field.attrs:
        values = np.atleast_1d(field.attrs["flag_values"]).astype(int)
        return dict(zip(values.tolist(), str(field.attrs["flag_meanings"]).split()))
    return spec.get("classes")


def _get_norm(radar: xr.Dataset, moment: str, data: np.ndarray, spec: MomentSpec):
    """Build the (data, cmap, norm, colorbar ticks, colorbar tick labels) for a moment from its MomentSpec."""
    kind = spec.get("norm") or "linear"
    vmin, vmax = spec.get("vmin"), spec.get("vmax")

    if kind == "categorical":
        classes = get_classes(radar[moment], spec)
        if classes is None:
            classes = {int(v): str(int(v)) for v in np.unique(data[np.isfinite(data)])} or {0: "0"}
        values = np.array(list(classes.keys()))
        # Map class values onto 0..N-1 so each class gets one colour and one evenly spaced colorbar slot.
        index = np.full(data.shape, np.nan)
        for i, value in enumerate(values):
            index[data == value] = i
        if spec.get("cmap") is not None:
            cmap = plt.get_cmap(spec["cmap"], len(values))
        else:
            fallback = plt.get_cmap("tab20")
            colours = [CLASS_COLOURS.get(label, fallback(i % fallback.N)) for i, label in enumerate(classes.values())]
            cmap = mcolors.ListedColormap(colours)
        norm = mcolors.BoundaryNorm(np.arange(len(values) + 1) - 0.5, cmap.N)
        return index, cmap, norm, np.arange(len(values)), list(classes.values())

    cmap = plt.get_cmap(spec.get("cmap") or "viridis")
    if kind == "diverging":
        if vmin is None or vmax is None:
            # Symmetric around 0: use the Nyquist velocity when available, else the data extent.
            vlim = radar.attrs.get("NI") if moment.startswith("VRAD") else None
            if vlim is None:
                vlim = np.nanmax(np.abs(data)) if np.isfinite(data).any() else 1.0
            vmin, vmax = -vlim, vlim
        vcenter = 0.0 if vmin < 0 < vmax else (vmin + vmax) / 2
        return data, cmap, mcolors.TwoSlopeNorm(vcenter=vcenter, vmin=vmin, vmax=vmax), None, None

    return data, cmap, mcolors.Normalize(vmin=vmin, vmax=vmax), None, None


def plot(
        rset: xr.Dataset | List[xr.Dataset], 
        moments: List[str] | None = None, 
        sweep: int = 0, 
        xlims=[-150e3, 150e3], 
        ylims=[-150e3, 150e3],
        nrings: int = 3,
        ncols: int = 3,
    ):
    if isinstance(rset, xr.Dataset):
        radar: Dataset = rset
    else:
        radar: Dataset = rset[sweep]

    if moments is None:
        for m in [DEFAULT_DP, DEFAULT_DOP, DEFAULT_SP]:
            try:
                moments = check_moments(radar, m)
                break
            except ValueError as e:
                print(f"Warning: {e}")
        else:
            raise ValueError("None of the default AURA moment sets are available in the radar dataset, provide `moments` explicitly.")
    else:
        moments = check_moments(radar, moments)

    th = np.linspace(0, 2 * np.pi, 361)
    maxrange = max(abs(xlims[0]), abs(xlims[1]), abs(ylims[0]), abs(ylims[1]))
    ringradii = np.linspace(maxrange / nrings, maxrange, nrings)

    # Plot in km, limits are given in m.
    x = radar.x.values / 1e3
    y = radar.y.values / 1e3
    central_lon = radar.attrs["longitude"]
    central_lat = radar.attrs["latitude"]
    date = pd.Timestamp(radar.attrs["date"])

    ncols = min(ncols, len(moments))
    nrows = int(np.ceil(len(moments) / ncols)) 
    fig, axes = plt.subplots(
        nrows, ncols, figsize=(4.5 * ncols, 4 * nrows), sharex=True, sharey=True, squeeze=False, layout="compressed"
    )

    for ax, moment, abc in zip(axes.flat, moments, string.ascii_lowercase):
        spec: MomentSpec = get_spec(moment)
        data = radar[moment].values.astype(float)
        if spec.get("mask_below") is not None:
            data = np.where(data < spec["mask_below"], np.nan, data)
        data, cmap, norm, ticks, ticklabels = _get_norm(radar, moment, data, spec)
        data = np.ma.masked_invalid(data)

        with warnings.catch_warnings():
            # x/y are polar-gridded, hence not monotonic; cell edges are still correctly inferred along each ray.
            warnings.filterwarnings("ignore", message=".*not monotonically increasing or decreasing.*")
            im = ax.pcolormesh(x, y, data, cmap=cmap, norm=norm, shading="auto")

        # Colorbar in axes coordinates so it always spans the exact (aspect-adjusted) axes height.
        cax = ax.inset_axes([1.03, 0, 0.05, 1])
        cbar = fig.colorbar(im, cax=cax)
        cbar.set_label(spec.get("label") or moment)
        if ticks is not None:
            cbar.set_ticks(ticks, labels=ticklabels)
            cbar.ax.tick_params(length=0, labelsize="small")

        for radius in ringradii:
            ax.plot(radius * np.cos(th) / 1e3, radius * np.sin(th) / 1e3, "k--", linewidth=0.5)
        ax.axhline(0, color="k", linewidth=0.5, linestyle=":")
        ax.axvline(0, color="k", linewidth=0.5, linestyle=":")

        ax.set_xlim(xlims[0] / 1e3, xlims[1] / 1e3)
        ax.set_ylim(ylims[0] / 1e3, ylims[1] / 1e3)
        ax.set_aspect("equal", adjustable="box")
        ax.set_title(moment)
        ax.set_title(f"{abc}.", loc="left", fontweight="bold")

    for ax in axes.flat[len(moments):]:
        ax.set_visible(False)
    for ax in axes[-1, :]:
        ax.set_xlabel("x (km)")
    for ax in axes[:, 0]:
        ax.set_ylabel("y (km)")

    title = [radar.attrs.get("source", "")]
    if "elevation" in radar.coords:
        title.append(f"elevation {float(radar.elevation.values[0]):.1f}°")
    title.append(f"{date:%Y-%m-%d %H:%M} UTC")
    # A single column is too narrow for the full title on one line.
    fig.suptitle(("\n" if ncols == 1 else " | ").join(t for t in title if t))

    return fig



