import requests
import json
import argparse
import logging
from neo4j import GraphDatabase

def setup_logger(log_file='app.log', level=logging.INFO):
    logger = logging.getLogger("VK-Neo4j")
    handler = logging.FileHandler(log_file)
    formatter = logging.Formatter('%(asctime)s - %(levelname)s - %(message)s')
    handler.setFormatter(formatter)
    logger.addHandler(handler)
    logger.setLevel(level)
    return logger

logger = setup_logger()

class Neo4jDatabase:
    def __init__(self, uri, user, password):
        self.driver = GraphDatabase.driver(uri, auth=(user, password))
    
    def close(self):
        self.driver.close()

    def add_user(self, user_data):
        with self.driver.session() as session:
            session.execute_write(self._create_user, user_data)
    
    @staticmethod
    def _create_user(tx, user_data):
        tx.run(
            """
            MERGE (u:User {id: $id})
            SET u.screen_name = $screen_name, u.name = $name,
                u.sex = $sex, u.home_town = $home_town
            """,
            **user_data
        )

    def add_group(self, group_data):
        with self.driver.session() as session:
            session.execute_write(self._create_group, group_data)

    @staticmethod
    def _create_group(tx, group_data):
        tx.run(
            """
            MERGE (g:Group {id: $id})
            SET g.name = $name, g.screen_name = $screen_name
            """,
            **group_data
        )

    def add_relationship(self, from_user, to_user, rel_type):
        with self.driver.session() as session:
            session.execute_write(self._create_relationship, from_user, to_user, rel_type)

    def add_relationship_sub(self, from_user, to_group, rel_type):
        with self.driver.session() as session:
            session.execute_write(self._create_relationship_sub, from_user, to_group, rel_type)

    @staticmethod
    def _create_relationship(tx, from_user, to_user, rel_type):
        query = f"""
        MATCH (a:User {{id: {from_user}}}), (b:User {{id: {to_user}}})
        MERGE (a)-[:{rel_type}]->(b)
        """
        tx.run(query, from_user=from_user, to_user=to_user)

    @staticmethod
    def _create_relationship_sub(tx, from_user, to_group, rel_type):
        query = f"""
        MATCH (a:User {{id: {from_user}}}), (b:Group {{id: {to_group}}})
        MERGE (a)-[:{rel_type}]->(b)
        """
        tx.run(query, from_user=from_user, to_group=to_group)

    # Методы для выполнения запросов
    def count_users(self):
        with self.driver.session() as session:
            result = session.run("MATCH (u:User) RETURN count(u) AS total_users")
            return result.single()["total_users"]

    def count_groups(self):
        with self.driver.session() as session:
            result = session.run("MATCH (g:Group) RETURN count(g) AS total_groups")
            return result.single()["total_groups"]

    def top_users_by_followers(self, limit=5):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (u:User)<-[:Follow]-(follower:User)
                RETURN u.id AS user_id, u.name AS name, count(follower) AS followers_count
                ORDER BY followers_count DESC
                LIMIT $limit
                """, limit=limit)
            return result.values()

    def top_groups_by_subscribers(self, limit=5):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (g:Group)<-[:Subscribe]-(user:User)
                RETURN g.id AS group_id, g.name AS name, count(user) AS subscribers_count
                ORDER BY subscribers_count DESC
                LIMIT $limit
                """, limit=limit)
            return result.values()

    def mutual_followers(self):
        with self.driver.session() as session:
            result = session.run(
                """
                MATCH (u:User)-[:Follow]->(v:User), (v)-[:Follow]->(u)
                RETURN u.id AS user1_id, v.id AS user2_id
                """
            )
            return result.values()

# VK API функции
def get_user_info(user_id, token):
    url = f"https://api.vk.com/method/users.get?user_ids={user_id}&fields=followers_count,counters,city,screen_name,sex,home_town&access_token={token}&v=5.131"
    user_data = requests.get(url).json().get('response', [{}])[0]
    return {
        "id": user_data.get("id"),
        "screen_name": user_data.get("screen_name"),
        "name": f"{user_data.get('first_name')} {user_data.get('last_name')}",
        "sex": user_data.get("sex"),
        "home_town": user_data.get("city", {}).get("title", "")
    }

def get_followers(user_id, token):
    url = f"https://api.vk.com/method/users.getFollowers?user_id={user_id}&count=1000&access_token={token}&v=5.131"
    response = requests.get(url)
    return response.json()

def get_subscriptions(user_id, token):
    url = f"https://api.vk.com/method/users.getSubscriptions?user_id={user_id}&access_token={token}&v=5.131"
    response = requests.get(url)
    return response.json()

def get_group_info(group_id, token):
    url = f"https://api.vk.com/method/groups.getById?group_id={group_id}&access_token={token}&v=5.131"
    response = requests.get(url).json()
    group_data = response.get('response', [{}])[0]
    return {
        "id": group_data.get("id"),
        "name": group_data.get("name"),
        "screen_name": group_data.get("screen_name")
    }

def save_to_json(data, output_path):
    with open(output_path, 'w', encoding='utf-8') as f:
        json.dump(data, f, ensure_ascii=False, indent=4)
    logger.info(f"Data saved to {output_path}")

def collect_followers_and_subscriptions(user_id, token, depth=2):
    data = {}
    queue = [(user_id, 0)]
    visited = set()

    while queue:
        current_user, level = queue.pop(0)
        if level >= depth:
            continue
        if current_user in visited:
            continue
        visited.add(current_user)

        try:
            followers = get_followers(current_user, token).get('response', {}).get('items', [])
            subscriptions = get_subscriptions(current_user, token).get('response', {})
            user_subscriptions = subscriptions.get('users', {}).get('items', [])
            group_subscriptions = subscriptions.get('groups', {}).get('items', [])
            data[current_user] = {
                'followers': followers,
                'user_subscriptions': user_subscriptions,
                'group_subscriptions': group_subscriptions
            }
            queue.extend([(f, level + 1) for f in followers])
            queue.extend([(s, level + 1) for s in user_subscriptions])
        except Exception as e:
            logger.error(f"Error fetching data for user {current_user}: {e}")
    return data

def save_data_to_neo4j(db, data, token):
    for user_id, connections in data.items():
        user_node = get_user_info(user_id, token)
        db.add_user(user_node)
        
        for follower_id in connections['followers']:
            user_node = get_user_info(follower_id, token)
            db.add_user(user_node)
            db.add_relationship(follower_id, user_id, "Follow")
        for subscription_id in connections['user_subscriptions']:
            user_node = get_user_info(subscription_id, token)
            db.add_user(user_node)
            db.add_relationship(user_id, subscription_id, "Follow")
        for group_id in connections['group_subscriptions']:
            group_data = get_group_info(group_id, token)
            db.add_group(group_data)
            db.add_relationship_sub(user_id, group_id, "Subscribe")

def main(args, token):
    db = Neo4jDatabase(uri="Подключение к вашей бд", user="neo4j", password="Пароль от вашей бд")
    
    if args.query == "count_users":
        print("Total users:", db.count_users())
    elif args.query == "count_groups":
        print("Total groups:", db.count_groups())
    elif args.query == "top_users":
        top_users = db.top_users_by_followers(limit=5)
        print("Top 5 users by followers:")
        for user_id, name, followers_count in top_users:
            print(f"ID: {user_id}, Name: {name}, Followers: {followers_count}")
    elif args.query == "top_groups":
        top_groups = db.top_groups_by_subscribers(limit=5)
        print("Top 5 groups by subscribers:")
        for group_id, name, subscribers_count in top_groups:
            print(f"ID: {group_id}, Name: {name}, Subscribers: {subscribers_count}")
    elif args.query == "mutual_followers":
        mutual = db.mutual_followers()
        print("Users who follow each other:")
        for user1_id, user2_id in mutual:
            print(f"User {user1_id} <-> User {user2_id}")
    else:
        data = collect_followers_and_subscriptions(args.user_id, token, depth=1)
        save_to_json(data, args.output)
        print(f"Data saved to {args.output}")
        try:
            save_data_to_neo4j(db, data, token)
            logger.info("Data successfully saved to Neo4j database")
        except Exception as e:
            logger.error(f"Error saving to Neo4j: {e}")
    
    db.close()

if __name__ == "__main__":
    parser = argparse.ArgumentParser(description="VK Data Collector with Neo4j")
    parser.add_argument('--user_id', type=str, default='303214441', help="VK user ID")
    parser.add_argument('--output', type=str, default='vk_data.json', help="Output JSON file path")
    parser.add_argument('--query', type=str, choices=['count_users', 'count_groups', 'top_users', 'top_groups', 'mutual_followers'],
                        help="Query type for Neo4j database")
    args = parser.parse_args()

    token = 'Ваш токен VK'
    main(args, token)
