import requests
import json
import argparse
import os

def get_user_info(user_id, token):
    url = f"https://api.vk.com/method/users.get?user_ids={user_id}&fields=followers_count,counters&access_token={token}&v=5.131"
    response = requests.get(url)
    return response.json()

def get_followers(user_id, token):
    url = f"https://api.vk.com/method/users.getFollowers?user_id={user_id}&count=1000&access_token={token}&v=5.131"
    response = requests.get(url)
    return response.json()

def get_subscriptions(user_id, token):
    url = f"https://api.vk.com/method/users.getSubscriptions?user_id={user_id}&access_token={token}&v=5.131"
    response = requests.get(url)
    return response.json()

def save_to_json(data, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)

def main(user_id, output_path, token):
    user_info = get_user_info(user_id, token)
    followers = get_followers(user_id, token)
    subscriptions = get_subscriptions(user_id, token)

    data = {
        "user_info": user_info,
        "followers": followers,
        "subscriptions": subscriptions
    }

    save_to_json(data, output_path)
    print(f"Данные сохранены в файл {output_path}")

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK Data Collector")
    parser.add_argument('--user_id', type=str, default='303214441', help="VK user ID (default is your user ID)")
    parser.add_argument('--output', type=str, default='vk_data.json', help="Output JSON file path")
    args = parser.parse_args()

    token = 'Ваш токен VK'
    main(args.user_id, args.output, token)
