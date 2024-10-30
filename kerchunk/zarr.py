import asyncio
import fsspec
from fsspec.implementations.reference import LazyReferenceMapper

import kerchunk.utils

from zarr.abc.store import Store as ZarrStore
from zarr.core.buffer import default_buffer_prototype
from zarr.storage import make_store_path

def single_zarr(
    uri_or_store,
    storage_options=None,
    inline_threshold=100,
    inline=None,
    out=None,
):
    """kerchunk-style view on zarr mapper

    This is a similar process to zarr's consolidate_metadata, but does not
    need to be held in the original file tree. You do not need zarr itself
    to do this.

    This is useful for testing, so that we can pass hand-made zarrs to combine.

    Parameters
    ----------
    uri_or_store: str or dict-like
    storage_options: dict or None
        given to fsspec
    out: dict-like or None
        This allows you to supply an fsspec.implementations.reference.LazyReferenceMapper
        to write out parquet as the references get filled, or some other dictionary-like class
        to customise how references get stored

    Returns
    -------
    reference dict like
    """
    if isinstance(uri_or_store, str):
        uri_or_store = fsspec.get_mapper(uri_or_store, **(storage_options or {}))

    refs = out or {}
    if isinstance(uri_or_store, ZarrStore):
        # enable compatibility with zarr v3 Store instances
        store = uri_or_store
        keys = asyncio.run(kerchunk.utils.consume_async_gen(store.list()))
        for k in keys:
            if k.startswith("."):
                refs[k] = asyncio.run(store.get(k, prototype=default_buffer_prototype()))
            else:
                refs[k] = [
                    str(asyncio.run(
                        make_store_path(store, path=k, storage_options=storage_options)
                    ))
                ]
    else:
        mapper = uri_or_store
        if isinstance(mapper, fsspec.FSMap) and storage_options is None:
            storage_options = mapper.fs.storage_options
        for k in mapper:
            if k.startswith("."):
                refs[k] = mapper[k]
            else:
                refs[k] = [fsspec.utils._unstrip_protocol(mapper._key_to_str(k), mapper.fs)]

    from kerchunk.utils import do_inline

    inline_threshold = inline or inline_threshold
    if inline_threshold:
        refs = do_inline(refs, inline_threshold, remote_options=storage_options)
    if isinstance(refs, LazyReferenceMapper):
        refs.flush()
    refs = kerchunk.utils.consolidate(refs)
    return refs


ZarrToZarr = kerchunk.utils.class_factory(single_zarr)
