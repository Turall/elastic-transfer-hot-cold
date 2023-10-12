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
INDICES = dict()


class LIMITS(float, Enum):
    HOT_TIER_SIZE_LIMIT = 600.0
    COLD_TIER_SIZE_LIMIT = 1200.0
    HOT_INDEX_SIZE_LIMIT = 100


def convert_size_to_bytes(size):
    multipliers = {
        "kb": 1024,
        "mb": 1024 * 1024,
        "gb": 1024 * 1024 * 1024,
        "tb": 1024 * 1024 * 1024 * 1024,
    }

    for suffix in multipliers:
        if size.lower().endswith(suffix):
            return int(float(size[0 : -len(suffix)])) * multipliers[suffix]
    else:
        if size.lower().endswith("b"):
            return int(size[0:-1])

    try:
        return int(size)
    except ValueError:
        print("Malformed input!")


async def get_index_details():
    indices = await es.indices.get(index="logstash-*")
    index_info = indices.body
    index_names = index_info.keys()
    for i in index_names:
        INDICES[i] = {"settings": index_info[i]["settings"]}


async def get_indexes_sizes() -> list[tuple]:
    cluster_info = await es.cat.indices(index="logstash-*", s="index")
    index_info = cluster_info.body
    all_indexes = index_info.split("\n")
    all_indexes.pop()
    all_indexes = list(map(lambda index: index.split(), all_indexes))
    for i in all_indexes:
        INDICES[i[2]]["size"] = int(convert_size_to_bytes(i[-1]))


def build_index_data():
    indexes = []
    for k, v in INDICES.items():
        indexes.append({"size": v["size"], "settings": v["settings"], "index_name": k})
    return indexes


def parse_index_sizes(indexes_info: list[tuple]):
    val = 0
    for v in indexes_info:
        val += v.get("size")
    return val


def convert_byte_to_gb(size):
    return size / (1 << 30)


async def get_index_settings(index: dict):
    index_settings = index["settings"]["index"].pop("routing")
    return index_settings["allocation"]["include"]["_tier_preference"]


async def split_hot_cold_indexes(indexes):
    for i in indexes:
        tier = await get_index_settings(i)
        if tier == "data_content":
            HOT_TIER.append(i)
        else:
            COLD_TIER.append(i)


def calculate_tier_size(indexes: list[dict]):
    sizes = parse_index_sizes(indexes)
    return convert_byte_to_gb(sizes)


# def calculate_cold_tier_size():
#     sizes = parse_index_sizes(COLD_TIER)
#     return convert_byte_to_gb(sizes)


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
    size = 0
    index_count = 0
    for i in HOT_TIER:
        size += i["size"]
        index_count += 1
        size = convert_byte_to_gb(size)
        if (
            size > LIMITS.HOT_INDEX_SIZE_LIMIT
            and (size - LIMITS.HOT_INDEX_SIZE_LIMIT) > 50
        ):
            break

    indexes = HOT_TIER[:index_count]
    return indexes


async def transfer_index_from_hot_to_cold(hot_indexes_for_transfer: list[dict]):
    print("START transfer_index_from_hot_to_cold")
    for i in hot_indexes_for_transfer:
        await update_index_settings(i["index_name"])


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
    cold_index_size = calculate_tier_size(COLD_TIER)
    sum_of_with_hot_tier = cold_index_size + hot_index_sizes
    if sum_of_with_hot_tier <= LIMITS.COLD_TIER_SIZE_LIMIT:
        return True
    diff = sum_of_with_hot_tier - cold_index_size
    return await check_and_remove_cold_indexes(diff)


async def main():
    await get_index_details()
    await get_indexes_sizes()
    indexes = build_index_data()
    await split_hot_cold_indexes(indexes)
    print(calculate_tier_size(HOT_TIER))
    print(calculate_tier_size(COLD_TIER))
    transfer_hot_indexes = get_transferable_hot_indexes()
    if transfer_hot_indexes:
        hot_index_size = calculate_tier_size(transfer_hot_indexes)
        if transfer_hot_indexes and await check_cold_tier_limits(hot_index_size):
            print(transfer_hot_indexes)
            print(await check_cold_tier_limits(hot_index_size))
            await transfer_index_from_hot_to_cold(transfer_hot_indexes)
            print("Success!")
            return
    print("Transferable indexes not found!")


if __name__ == "__main__":
    try:
        asyncio.run(main())
    finally:
        print(asyncio.run(es.close()))
