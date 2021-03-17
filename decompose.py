from __future__ import annotations

import asyncio as aio
import aurcore
import asyncpg
import aurflux
import typing as ty
import collections as clc
import TOKENS
import itertools as itt
import re

EMOJI_REGEX = re.compile("<(?P<animated>a)?:(?P<name>[0-9a-zA-Z_]{2,32}):(?P<id>[0-9]{15,21})>")

if ty.TYPE_CHECKING:
   import discord
   import datetime

def merge_dicts(dicts):
   merged_dict = clc.defaultdict(list)
   [[merged_dict[k].append(v) for k, v in d.items()] for d in dicts]
   return merged_dict


def grouper(iterable, n):
   return zip(*([iter(iterable)] * n))


def build_insert(
      data: ty.Union[dict, ty.List[dict]],
      table: str
) -> ty.Tuple[ty.Optional[str], ty.Optional[str]]:
   if isinstance(data, dict):
      columns = ','.join(data.keys())
      excluded_columns = ','.join(f"EXCLUDED.{key}" for key in data.keys())
      values = ', '.join(f'${i}' for i in range(1, len(data) + 1))
      return f"INSERT INTO {table} ({columns}) values ({values}) ON CONFLICT ({list(data.keys())[0]}) DO UPDATE SET ({columns}) = ({excluded_columns});", data.values()
   if data:
      merged = merge_dicts(data)
      columns = ','.join(merged.keys())

      excluded_columns = ','.join(f"EXCLUDED.{key}" for key in merged.keys())
      value_indexes = grouper(range(1, len(merged) * len(list(merged.values())[0]) + 1), len(merged))
      values = ",".join([f"({','.join(f'${i}' for i in clump)})" for clump in value_indexes])
      return (f"INSERT INTO {table} ({columns}) values {values} ON CONFLICT ({list(merged.keys())[0]}) DO UPDATE SET ({columns}) = ({excluded_columns});",
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
      "has_mentions" : bool(m.mentions),
      "emoji_ids"    : [int(match.group("id")) for match in EMOJI_REGEX.finditer(m.content) if match]

   }


def emoji(e: discord.Emoji):
   emoji_info = {
      "emoji_id"  : e.id,
      "url"       : str(e.url),
      "created_at": e.created_at,
      "guild_id"  : e.guild.id,
      "name"      : e.name,

   }
   if e.user:
      emoji_info["creator_id"] = e.user.id
   # print(emoji_info)
   return emoji_info


def user(m: discord.User):
   return {
      "user_id": m.id,
      "name"   : m.name
   }


def mentions(m: discord.Message):
   return ([{"message_id": m.id, "type": "channel", "author_id": m.author.id, "target_id": target} for target in m.raw_channel_mentions] +
           [{"message_id": m.id, "type": "role", "author_id": m.author.id, "target_id": target, } for target in m.raw_role_mentions] +
           [{"message_id": m.id, "type": "user", "author_id": m.author.id, "target_id": target, } for target in m.raw_mentions])
