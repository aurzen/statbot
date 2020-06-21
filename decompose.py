from __future__ import annotations

import asyncio as aio
import aurcore
import asyncpg
import aurflux
import typing as ty
import collections as clc
import TOKENS
import itertools as itt

if ty.TYPE_CHECKING:
    import discord
    import datetime

    FieldTypes = ty.Union[int, str, datetime.datetime]
    SingleValue: ty.TypeAlias = ty.Dict[str, FieldTypes]
    MultiValue: ty.TypeAlias = ty.Dict[str, ty.List[FieldTypes]]


def merge_dicts(dicts):
    print(f"merging: {dicts}")
    merged_dict = clc.defaultdict(list)
    [[merged_dict[k].append(v) for k, v in d.items()] for d in dicts]
    return merged_dict


def grouper(iterable, n):
    return zip(*([iter(iterable)] * n))


def build_insert(data: ty.Union[dict, ty.List[dict]], table: str):
    if isinstance(data, dict):
        columns = ','.join(data.keys())
        values = ', '.join(f'${i}' for i in range(1, len(data) + 1))
        return f"INSERT INTO {table} ({columns}) values ({values}) ON CONFLICT DO NOTHING;", data.values()
    if data:
        merged = merge_dicts(data)
        columns = ','.join(merged.keys())
        value_indexes = grouper(range(1, len(merged) * len(list(merged.values())[0]) + 1), len(merged))
        values = ",".join([f"({','.join(f'${i}' for i in clump)})" for clump in value_indexes])
        return (f"INSERT INTO {table} ({columns}) values {values} ON CONFLICT DO NOTHING;",
                list(itt.chain.from_iterable(zip(*merged.values()))))
    return None, None


def message(m: discord.Message):
    return {
        "message_id"   : m.id,
        "author_id"    : m.author.id,
        "content"      : m.content,
        "clean_content": m.clean_content,
        "embeds"       : ";".join([embed.url for embed in m.embeds if embed.url]),
        "channel_id"   : m.channel.id,
        "guild_id"     : m.guild.id,
        "created_at"   : m.created_at,
        "has_mentions" : bool(m.mentions)

    }


def mentions(m: discord.Message):
    return ([{"type": "channel", "author_id": m.author.id, "target_id": target, "message_id": m.id} for target in m.raw_channel_mentions] +
            [{"type": "role", "author_id": m.author.id, "target_id": target, "message_id": m.id} for target in m.raw_role_mentions] +
            [{"type": "user", "author_id": m.author.id, "target_id": target, "message_id": m.id} for target in m.raw_mentions])
