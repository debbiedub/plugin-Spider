import asyncio
import fcp3.anode
import fcp3.node
import os.path
import tempfile
import yaml
from dataclasses import dataclass
from functools import lru_cache
from typing import Any, List, Dict, Tuple, TypeVar


TARGET_SIZE = 1024 * 16

MIN_SIZE = TARGET_SIZE
MAX_SIZE = TARGET_SIZE * 2


@dataclass
class BinInfo(object):
    uri: object
    value: int


@dataclass(frozen=True)
class FreenetURI(object):
    key: str


@dataclass(frozen=True)
class TermPageEntry(object):
    """We don't need to unpack or pack this one."""
    value: Any


def freeneturi_constructor(loader, node):
    assert isinstance(node, yaml.ScalarNode)
    assert node.tag == "!FreenetURI"
    assert isinstance(node.value, str)
    return FreenetURI(node.value)


def bininfo_constructor(loader, node):
    assert isinstance(node, yaml.MappingNode)
    assert node.tag == "!BinInfo"
    assert isinstance(node.value, list)
    assert isinstance(node.value[0], tuple)
    assert len(node.value) == 1
    key, value = node.value[0]
    return BinInfo(loader.construct_object(key),
                   loader.construct_object(value))


def termpageentry_constructor(loader, node: yaml.ScalarNode) -> TermPageEntry:
    assert isinstance(node, yaml.MappingNode)
    assert node.tag == "!TermPageEntry"
    new_d = dict()
    for t in node.value:
        new_d[loader.construct_object(t[0])] = loader.construct_object(t[1])
    return TermPageEntry(new_d)


def bininfo_representer(dumper, data: BinInfo):
    assert isinstance(data, BinInfo)
    return dumper.represent_mapping(u'!BinInfo', [(data.uri, data.value)])


def freeneturi_representer(dumper, data):
    assert isinstance(data, FreenetURI)
    return dumper.represent_scalar(u'!FreenetURI', data.key)


def termpageentry_representer(dumper, data: TermPageEntry):
    assert isinstance(data, TermPageEntry)
    return dumper.represent_mapping(u'!TermPageEntry', data.value)


yaml.add_constructor(u"!BinInfo", bininfo_constructor)
yaml.add_constructor(u"!FreenetURI", freeneturi_constructor)
yaml.add_constructor(u"!TermPageEntry", termpageentry_constructor)
yaml.add_representer(BinInfo, bininfo_representer)
yaml.add_representer(FreenetURI, freeneturi_representer)
yaml.add_representer(TermPageEntry, termpageentry_representer)

cache_dir = '/home/debbiedub/Freenet/library-spider-pushed-data-cache'


T1 = TypeVar('T1')
T2 = TypeVar('T2')


def convert_to_list(ordered_mapping: Dict[T1, T2]) -> \
        List[Tuple[T1, T2]]:
    return [(key, val) for key, val in ordered_mapping.items()]


@lru_cache
def get_contents_with_size(uri: FreenetURI) -> Tuple[Dict[str, Any], float]:
    filename = os.path.join(cache_dir, uri.key)
    if not os.path.exists(filename):
        filename = os.path.join('.', uri.key)
    result = dict()
    with open(filename) as stream:
        for n, y in enumerate(yaml.full_load_all(stream)):
            result[n] = y
    assert len(result) == 1
    return result[0], os.path.getsize(filename) / len(result[0])


def get_contents(uri: FreenetURI) -> Dict[str, Any]:
    return get_contents_with_size(uri)[0]


def is_leaf(uri: FreenetURI) -> bool:
    if ('ttab' in get_contents(uri) and
            'subnodes' in get_contents(uri)['ttab']):
        return False
    if ('subnodes' in get_contents(uri)):
        return False
    return True


def join_leafs(
        first_uri: Tuple[FreenetURI, int] | Tuple[Dict[str, Any], int],
        entry: Tuple[str, BinInfo],
        second_uri: Tuple[FreenetURI, int] | Tuple[Dict[str, Any], int]) \
        -> Tuple[Dict[str, Any], int]:
    result = dict()
    if isinstance(first_uri[0], FreenetURI):
        first: Dict[str, Any] = get_contents(first_uri[0])
    else:
        first = first_uri[0]
    if isinstance(second_uri[0], FreenetURI):
        second: Dict[str, Any] = get_contents(second_uri[0])
    else:
        second = second_uri[0]
    result['lkey'] = first['lkey']
    result['rkey'] = second['rkey']
    entries_list: List[Tuple[str, BinInfo]] = list()
    for entry in (convert_to_list(first['entries']) +
                  [entry] +
                  convert_to_list(second['entries'])):
        term, info = entry
        if info.value > 0:
            entries_list.append(entry)
    result['entries'] = convert_to_dict(entries_list)
    return result, len(entries_list)


def join_non_leafs(subnodes0: Tuple[FreenetURI, int],
                   entries: Tuple[str, BinInfo],
                   subnodes1: Tuple[FreenetURI, int]) -> \
        Tuple[List[Tuple[str, BinInfo]], List[Tuple[FreenetURI, int]]]:
    """Builds a huge single-level list that will later be joined."""
    s0 = get_contents(subnodes0[0])
    assert 'entries' in s0
    assert 'subnodes' in s0
    s1 = get_contents(subnodes1[0])
    assert 'entries' in s1
    assert 'subnodes' in s1
    return (convert_to_list(s0['entries']) +
            [entries] +
            convert_to_list(s1['entries']),
            convert_to_list(s0['subnodes']) + convert_to_list(s1['subnodes']))


def convert_to_dict(old: List[Tuple[Any, Any]]) -> Dict[Any, Any]:
    new = dict()
    for key, val in old:
        new[key] = val
    return new


def correctly_formed(entries: List[Tuple[str, BinInfo]],
                     subnodes: List[Tuple[FreenetURI, int]]) -> bool:
    assert len(entries) + 1 == len(subnodes)
    assert len(entries) < MAX_SIZE
    for x in subnodes:
        if x[1] < MIN_SIZE:
            return False
        assert x[1] < MAX_SIZE
    return True


def sizes_bigger_than(subnodes: List[Tuple[FreenetURI, int]], limit) -> bool:
    sum = 0
    for subnode in subnodes:
        sum += subnode[1]
        if sum > limit:
            return True
    return False


async def write_yaml_dump(anode: fcp3.anode.ANode,
                          tg: asyncio.TaskGroup,
                          data: Dict[str, Any]) -> FreenetURI:
    with tempfile.NamedTemporaryFile(dir='.', delete=False) as file:
        file_name = file.name
        file.write(yaml.dump(data).encode())
    new_uri = FreenetURI(
        await anode.put2(tg, "CHK@", data=open(file_name, 'rb').read()))
    os.rename(file_name, new_uri.key)
    return new_uri


async def add_new_uri(anode: fcp3.anode.ANode,
                      tg: asyncio.TaskGroup,
                      subnodes: List[Tuple[FreenetURI, int]],
                      pos: int,
                      data: Any,
                      size: int) -> None:
    print('Filling', pos, 'size', size, 'total', len(subnodes))
    new_uri = await write_yaml_dump(anode, tg, data)
    subnodes[pos] = (new_uri, size)
    print('Filled', pos, 'size', size, 'total', len(subnodes), new_uri)


async def process_one_set_of_entries(
        anode: fcp3.anode.ANode,
        tg: asyncio.TaskGroup,
        new_root_data: Dict[str, Any],
        subset_of_enumerated_entries: List[Tuple[int, Tuple[str, BinInfo]]],
        entries: List[Tuple[str, BinInfo]]) -> None:
    new_uri = await write_yaml_dump(anode, tg, new_root_data)
    for i, t in subset_of_enumerated_entries:
        s, b = t
        entries[i] = (s, BinInfo(new_uri, b.value),)


async def process_entries(anode: fcp3.anode.ANode,
                          tg: asyncio.TaskGroup,
                          entries: List[Tuple[str, BinInfo]]) -> None:
    """Move the contents of the entries into a new set of files/CHKs."""
    count_root_uri = 0
    async with asyncio.TaskGroup() as petg:
        new_root_data: Dict[str, Any] = dict()
        accumulated_size = 0.0
        subset_of_enumerated_entries: List[Tuple[int, Tuple[str, BinInfo]]] \
            = list()
        for e in enumerate(entries):
            subset_of_enumerated_entries.append(e)
            s, b = e[1]
            contents, average_size = get_contents_with_size(b.uri)
            new_root_data[s] = contents[s]
            accumulated_size += average_size
            if accumulated_size > 1_250_000.0:
                print(f'Start roots {count_root_uri+1}',
                      f'with estimated size {accumulated_size:.0f}',
                      f'({e[0]}/{len(entries)})')
                petg.create_task(
                    process_one_set_of_entries(
                        anode, tg, new_root_data,
                        subset_of_enumerated_entries, entries))
                await asyncio.sleep(1)
                new_root_data = dict()
                accumulated_size = 0
                subset_of_enumerated_entries = list()
                count_root_uri += 1
        if len(subset_of_enumerated_entries) > 0:
            print(f'Start last roots {count_root_uri+1}',
                  f'with estimated size {accumulated_size:.0f}')
            petg.create_task(
                process_one_set_of_entries(
                    anode, tg, new_root_data,
                    subset_of_enumerated_entries, entries))
            count_root_uri += 1
    print(f'Created {count_root_uri} root uris')


async def transform_index(anode: fcp3.anode.ANode,
                          tg: asyncio.TaskGroup,
                          uri: FreenetURI) -> FreenetURI:
    """Open and upload replacing the first in subnodes."""
    to_transform = 1

    l0 = get_contents(uri)

    where_to_replace = l0
    assert 'ttab' in where_to_replace
    l0['name'] = 'DebbiesIndex'
    l0['ownerName'] = 'Debora Wöpcke'
    l0['ownerEmail'] = (
        'debora_wpcke@' +
        't2ym4522mj5bndpph3wpxyqyyb77m6fo4byq64ac2pgmipzrhrfq.freemail')
    l0['ttab']['node_min'] = MIN_SIZE
    ttab = where_to_replace['ttab']

    assert 'entries' in ttab
    assert 'subnodes' in ttab

    input_entries: List[Tuple[str, BinInfo]] = \
        convert_to_list(ttab['entries'])
    input_subnodes: List[Tuple[FreenetURI, int]] = \
        convert_to_list(ttab['subnodes'])
    assert len(input_subnodes) == len(input_entries) + 1

    output_entries: List[Tuple[str, BinInfo]] = list()
    output_subnodes: List[Tuple[FreenetURI, int]] = list()

    first_to_handle = 0

    if len(input_entries) == 1:
        # This is the first change
        assert input_entries[0][1].value == 0

        output_entries, output_subnodes = \
            join_non_leafs(input_subnodes[0],
                           input_entries[0],
                           input_subnodes[1])
    else:
        # This is if we have already done the first one and thus
        # joined the two on the second level.

        output_entries = input_entries
        output_subnodes = input_subnodes

        # Skip the ones that already have the appropriate size
        while output_subnodes[first_to_handle][1] >= MIN_SIZE:
            first_to_handle += 1

        if first_to_handle >= len(output_subnodes):
            print('Nothing to do')
            return uri

    async with asyncio.TaskGroup() as output_subnodes_tg:
        while sizes_bigger_than(output_subnodes[first_to_handle:],
                                2 * MAX_SIZE):
            print(f'Join number {first_to_handle}')
            first_data = get_contents(output_subnodes[first_to_handle][0])
            first_size = output_subnodes[first_to_handle][1]
            while (first_size + output_subnodes[first_to_handle + 1][1] <
                   MAX_SIZE):
                first_data, first_size = join_leafs(
                    (first_data, first_size),
                    output_entries[first_to_handle],
                    output_subnodes[first_to_handle + 1])
                output_entries.pop(first_to_handle)
                output_subnodes.pop(first_to_handle + 1)
            data_entries_list = convert_to_list(first_data['entries'])
            await process_entries(anode, tg, data_entries_list)
            first_data['entries'] = convert_to_dict(data_entries_list)
            output_subnodes_tg.create_task(
                add_new_uri(anode, tg,
                            output_subnodes, first_to_handle,
                            first_data, first_size))
            await asyncio.sleep(1)
            first_to_handle += 1
            to_transform -= 1
            if not to_transform > 0:
                break

        # Do the last two, half of the rest in each
        if to_transform > 0:
            print('Proceeding with the last two')
            first_data = get_contents(output_subnodes[first_to_handle][0])
            first_size = output_subnodes[first_to_handle][1]
            for _ in range((len(output_subnodes) - first_to_handle) // 2):
                first_data, first_size = join_leafs(
                    (first_data, first_size),
                    output_entries[first_to_handle],
                    output_subnodes[first_to_handle + 1])
                output_entries.pop(first_to_handle)
                output_subnodes.pop(first_to_handle + 1)
            data_entries_list = convert_to_list(first_data['entries'])
            await process_entries(anode, tg, data_entries_list)
            first_data['entries'] = convert_to_dict(data_entries_list)
            output_subnodes_tg.create_task(
                add_new_uri(anode, tg,
                            output_subnodes, first_to_handle,
                            first_data, first_size))
            await asyncio.sleep(1)
            first_to_handle += 1

            first_data = get_contents(output_subnodes[first_to_handle][0])
            first_size = output_subnodes[first_to_handle][1]
            while first_to_handle + 1 < len(output_subnodes):
                first_data, first_size = join_leafs(
                    (first_data, first_size),
                    output_entries[first_to_handle],
                    output_subnodes[first_to_handle + 1])
                output_entries.pop(first_to_handle)
                output_subnodes.pop(first_to_handle + 1)
            data_entries_list = convert_to_list(first_data['entries'])
            await process_entries(anode, tg, data_entries_list)
            first_data['entries'] = convert_to_dict(data_entries_list)
            output_subnodes_tg.create_task(
                add_new_uri(anode, tg,
                            output_subnodes, first_to_handle,
                            first_data, first_size))

            for subnode in output_subnodes:
                assert subnode[1] >= MIN_SIZE

        for subnode in output_subnodes:
            assert subnode[1] <= MAX_SIZE

        assert len(output_subnodes) == len(output_entries) + 1
        # output_subnodes_tg completes so output_subnodes contain all URIs

    await process_entries(anode, tg, output_entries)

    ttab['entries'] = convert_to_dict(output_entries)
    ttab['subnodes'] = convert_to_dict(output_subnodes)
    ttab['size'] = len(output_entries) + sum([n[1] for n in output_subnodes])

    return await write_yaml_dump(anode, tg, l0)


class LimitedTaskGroup(asyncio.TaskGroup):
    """A taskgroup that runs a limited amount of tasks at the time.

    Whenever one of the running coroutines in the taskgroup are done,
    new ones are started.

    Also, the amount of runnable coroutines are printed.
    """
    def __init__(self, s):
        asyncio.TaskGroup.__init__(self)
        self.sem = asyncio.Semaphore(s)
        self.count = 0

    async def with_coro(self, coro):
        self.count += 1
        print(f'{self.count} tasks runnable')
        async with self.sem:
            await coro
        self.count -= 1
        print(f'{self.count} tasks remaining.')

    def create_task(self, coro, *, name=None, context=None):
        asyncio.TaskGroup.create_task(self, self.with_coro(coro),
                                      name=name, context=context)


async def main():
    start_from = 'CHK@Dr8Af4NKAveEWWBm~f9dzHS6gh~YhTN2xVuzzocYQe0,' + \
        'itvTlDQybz6V-aQlJMY21LSys1yqGebuetuTW5KAugU,AAMA--8'
    lastpushed_filename = 'lastpushed.chk'
    if os.path.exists(lastpushed_filename):
        start_from = open(lastpushed_filename, 'rt').read()
        print(f'Starting from {lastpushed_filename}')
    async with fcp3.anode.ANode() as anode:
        async with LimitedTaskGroup(5) as tg:
            print('Will transform the index in', start_from)
            new_index = await transform_index(anode, tg,
                                              FreenetURI(start_from))
            print(new_index)
        with open(lastpushed_filename, 'wt') as out:
            out.write(new_index.key)
        print('Upload done')
        print(await anode.put(
            "USK@Jp~R0u8QLeH2U1yeoT4PZ88tRtZwT6a2eYGoc~SRJn4," +
            "5nwpYIej7GDOmXYuWJRsT9MACVkKO7f04WcM7yuE2FU,AQECAAE/index.yml/0",
            redirect=new_index.key))


asyncio.run(main())
