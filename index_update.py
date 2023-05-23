import asyncio
import os
from enum import Enum

from elasticsearch import AsyncElasticsearch

config = {"basic_auth": (os.getenv("ELASTIC_USER"), os.getenv("ELASTIC_PASSWORD"))}

if os.getenv("ELASTIC_CLOUD_ID"):
    config["cloud_id"] = os.getenv("ELASTIC_CLOUD_ID")
elif os.getenv("ELASTIC_HOST"):
    config["hosts"] = os.getenv("ELASTIC_HOST")


es = AsyncElasticsearch(**config)

HOT_TIER = []
COLD_TIER = []


class LIMITS(float, Enum):
    HOT_TIER_SIZE_LIMIT = 600.0
    COLD_TIER_SIZE_LIMIT = 1200.0
    HOT_INDEX_SIZE_LIMIT = 4


async def get_indexes_info() -> list[tuple]:
    cluster_info = await es.cat.indices(index="logstash-*", s="index")
    index_info = cluster_info.body
    all_indexes = index_info.split("\n")
    all_indexes.pop()
    all_indexes = list(map(lambda index: index.split(), all_indexes))
    return list(map(lambda x: (x[2], x[-1]), all_indexes))


def parse_index_sizes(indexes_info: list[tuple]):
    return list(map(lambda x: float(x[-1].replace("gb", "")), indexes_info))


async def get_index_settings(index_name: str):
    settings = await es.indices.get_settings(index=index_name)
    settings = settings.body
    index_settings = settings[index_name]["settings"]["index"].pop("routing")
    return index_settings["allocation"]["include"]["_tier_preference"]


async def split_hot_cold_indexes(all_indexes):
    for i in all_indexes:
        tier = await get_index_settings(i[0])
        if tier == "data_content":
            HOT_TIER.append(i)
        else:
            COLD_TIER.append(i)


def calculate_hot_tier_size():
    sizes = parse_index_sizes(HOT_TIER)
    return sum(sizes)


def calculate_cold_tier_size():
    sizes = parse_index_sizes(COLD_TIER)
    return sum(sizes)


def get_free_size_of_cold_tier(cold_instance_size: float, limit: float):
    return limit - cold_instance_size


def get_size_hot_to_transfer(hot_instance_size: float, limit: float):
    return hot_instance_size - limit


async def update_index_settings(index_name: str):
    key_name = "index.routing.allocation.include._tier_preference"
    settings = {key_name: None}
    settings[key_name] = "data_cold"
    await es.indices.put_settings(settings=settings, index=index_name)


def get_transferable_hot_indexes():
    if len(HOT_TIER) > LIMITS.HOT_INDEX_SIZE_LIMIT:
        diff = int(len(HOT_TIER) - LIMITS.HOT_INDEX_SIZE_LIMIT)
        indexes = HOT_TIER[:diff]
        return indexes


async def transfer_index_from_hot_to_cold(hot_indexes_for_transfer: list[tuple]):
    print("START transfer_index_from_hot_to_cold")
    for i in hot_indexes_for_transfer:
        await update_index_settings(i[0])


async def check_and_remove_cold_indexes(index_size: float):
    print("START check_and_remove_cold_indexes -> ", index_size)
    removable_indexes = []
    size_ = 0
    for i in parse_index_sizes(COLD_TIER):
        size_ += i
        if size_ > index_size:
            removable_indexes.append(COLD_TIER.pop(0)[0])
            break
        removable_indexes.append(COLD_TIER.pop(0)[0])
    print("removable_indexes -> ", removable_indexes)
    if removable_indexes:
        for i in removable_indexes:
            await es.indices.delete(index=i)
        return True
    return False


async def check_cold_tier_limits(hot_index_sizes: float):
    print("START check_cold_tier_limits")
    cold_index_size = sum(parse_index_sizes(COLD_TIER))
    sum_of_with_hot_tier = cold_index_size + hot_index_sizes
    if sum_of_with_hot_tier <= LIMITS.COLD_TIER_SIZE_LIMIT:
        return True
    diff = sum_of_with_hot_tier - cold_index_size
    return await check_and_remove_cold_indexes(diff)


async def main():
    indexes = await get_indexes_info()
    await split_hot_cold_indexes(indexes)
    transfer_hot_indexes = get_transferable_hot_indexes()
    if transfer_hot_indexes:
        hot_index_size = sum(parse_index_sizes(transfer_hot_indexes))
        if transfer_hot_indexes and await check_cold_tier_limits(hot_index_size):
            await transfer_index_from_hot_to_cold(transfer_hot_indexes)
            print("Success!")
            return
    print("Transferable indexes not found!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        print(asyncio.run(es.close()))
