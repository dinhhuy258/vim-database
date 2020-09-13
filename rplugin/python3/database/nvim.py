from pynvim import Nvim


def init_nvim(nvim: Nvim) -> None:
    global _nvim
    _nvim = nvim
