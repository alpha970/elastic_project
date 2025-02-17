import asyncio
import aiohttp
import logging
from datetime import datetime, timezone
from typing import Dict, Optional
from elasticsearch import AsyncElasticsearch

# Configuration globale
REPOS = [
    "microsoft/vscode",
    "tensorflow/tensorflow",
    "facebook/react",
    "microsoft/TypeScript"
]
GITHUB_TOKEN = "remplace par token"  # Mets ici ton token GitHub
ELASTICSEARCH_URL = "http://localhost:9200"
INDEX_NAME = "github-events"

class GitHubEventMonitor:
    def __init__(self, repo: str, token: Optional[str] = None):
        self.repo = repo
        self.base_url = f"https://api.github.com/repos/{repo}/events"
        self.headers = {"Accept": "application/vnd.github.v3+json"}
        if token:
            self.headers["Authorization"] = f"Bearer {token}"

        self.processed_events = set()
        self.es = AsyncElasticsearch([ELASTICSEARCH_URL])

        logging.basicConfig(level=logging.INFO, format="%(asctime)s - %(levelname)s - %(message)s")
        self.logger = logging.getLogger(__name__)

    async def fetch_api_events(self):
        """
        Récupère les événements GitHub via l'API et les enregistre dans Elasticsearch.
        """
        async with aiohttp.ClientSession() as session:
            while True:
                try:
                    async with session.get(self.base_url, headers=self.headers) as response:
                        if response.status == 200:
                            events = await response.json()
                            for event in events:
                                await self.process_event(event)
                        else:
                            self.logger.error(f"Erreur API GitHub : {response.status} - {await response.text()}")
                except Exception as e:
                    self.logger.error(f"Erreur lors de la récupération des événements : {e}")

                await asyncio.sleep(10)  # Rafraîchissement toutes les 10 secondes

    async def process_event(self, event: Dict):
        """
        Traite un événement GitHub et l'envoie à Elasticsearch.
        """
        event_id = event.get("id")
        if event_id in self.processed_events:
            return  # Évite les doublons

        self.processed_events.add(event_id)
        timestamp = datetime.now(timezone.utc).isoformat()

        # Structure de données enrichie
        change = {
            "timestamp": timestamp,
            "repo": self.repo,
            "event_type": event.get("type"),
            "actor": event.get("actor", {}),
            "payload": self.filter_payload(event.get("payload", {})),
            "created_at": event.get("created_at"),
        }

        # Champs calculés en fonction du type d'événement
        if change["event_type"] == "PushEvent":
            change["commit_count"] = len(change["payload"].get("commits", []))
        elif change["event_type"] == "PullRequestEvent":
            change["is_merge"] = change["payload"].get("pull_request", {}).get("merged", False)

        self.logger.info(f"🔹 Nouvel événement GitHub : {change['event_type']}")
        await self.store_event(change)

    def filter_payload(self, payload: Dict) -> Dict:
        """
        Filtre le payload pour ne garder que les champs importants.
        """
        filtered_payload = {
            "action": payload.get("action"),
            "number": payload.get("number"),
        }

        # Informations spécifiques à IssueCommentEvent
        if "issue" in payload:
            filtered_payload["issue"] = {
                "title": payload["issue"].get("title"),
                "number": payload["issue"].get("number"),
                "state": payload["issue"].get("state"),
                "created_at": payload["issue"].get("created_at"),
                "updated_at": payload["issue"].get("updated_at"),
                "user": {
                    "login": payload["issue"].get("user", {}).get("login"),
                },
            }

        # Informations spécifiques à PullRequestEvent
        if "pull_request" in payload:
            filtered_payload["pull_request"] = {
                "title": payload["pull_request"].get("title"),
                "number": payload["pull_request"].get("number"),
                "state": payload["pull_request"].get("state"),
                "merged": payload["pull_request"].get("merged"),
                "user": {
                    "login": payload["pull_request"].get("user", {}).get("login"),
                },
            }

        # Informations spécifiques à PushEvent
        if "commits" in payload:
            filtered_payload["commits"] = [
                {
                    "sha": commit.get("sha"),
                    "message": commit.get("message"),
                }
                for commit in payload["commits"]
            ]

        # Informations spécifiques à IssueCommentEvent (commentaire)
        if "comment" in payload:
            filtered_payload["comment"] = {
                "body": payload["comment"].get("body"),
                "user": {
                    "login": payload["comment"].get("user", {}).get("login"),
                },
                "created_at": payload["comment"].get("created_at"),
            }

        return filtered_payload

    async def store_event(self, event: Dict):
        """
        Stocke un événement dans Elasticsearch.
        """
        try:
            await self.es.index(index=INDEX_NAME, document=event)
            self.logger.info("✅ Événement stocké dans Elasticsearch")
        except Exception as e:
            self.logger.error(f"❌ Erreur lors de l'enregistrement dans Elasticsearch : {e}")

    async def run(self):
        """
        Démarre la collecte des événements pour un dépôt.
        """
        await self.fetch_api_events()

async def main():
    # Créer une instance de monitor pour chaque dépôt
    tasks = []
    for repo in REPOS:
        monitor = GitHubEventMonitor(repo, GITHUB_TOKEN)  # Crée une nouvelle instance pour chaque dépôt
        tasks.append(monitor.run())  # Ajoute la tâche pour chaque dépôt

    # Lancer les tâches de collecte d'événements pour tous les dépôts
    await asyncio.gather(*tasks)

if __name__ == "__main__":
    asyncio.run(main())