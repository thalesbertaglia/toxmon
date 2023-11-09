import ast
import re
from dataclasses import dataclass, field
from typing import Any, Dict, List, Optional


@dataclass
class RedditParser:
    url_pattern: re.Pattern = field(default=re.compile(r"https?://\S+"), init=False)

    def parse_thread(self, thread_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extracts essential information from the thread JSON and returns a Pandas DataFrame.
        """
        thread_name: Optional[str] = thread_json.get("name", None)
        subreddit_name: Optional[str] = thread_json.get("subreddit_name_prefixed", None)
        title: Optional[str] = thread_json.get("title", None)
        author_name: Optional[str] = (
            thread_json.get("author", {}).get("name", None)
            if thread_json.get("author") != "None"
            else None
        )
        ups: int = thread_json.get("ups", 0)
        downs: int = thread_json.get("downs", 0)
        upvote_ratio: float = thread_json.get("upvote_ratio", 0.0)
        score: int = thread_json.get("score", 0)
        selftext: Optional[str] = thread_json.get("selftext", None)
        media: Optional[str] = thread_json.get("media", None)
        media_only: bool = thread_json.get("media_only", False)
        created_utc: int = thread_json.get("created_utc", 0)

        return {
            "thread_name": thread_name,
            "subreddit_name": subreddit_name,
            "title": title,
            "author_name": author_name,
            "ups": ups,
            "downs": downs,
            "upvote_ratio": upvote_ratio,
            "score": score,
            "selftext": selftext,
            "media": media,
            "media_only": media_only,
            "created_utc": created_utc,
        }

    def parse_comment(self, comment_json: Dict[str, Any]) -> Dict[str, Any]:
        """
        Extracts essential information from the comment JSON and returns a Pandas DataFrame.
        """
        return self._parse_comment_tree(comment_json)

    def _parse_comment_tree(
        self, comment: Dict[str, Any], parent_id: str = None
    ) -> List[Dict[str, Any]]:
        """
        Recursively parses the comment tree to extract information from both top-level comments and nested replies.
        """
        comment_data = [
            {
                "id": comment.get("id", None),
                "body": comment.get("body", None),
                "author": comment.get("author", {}).get("name", None)
                if comment.get("author")
                else None,
                "created_utc": comment.get("created_utc", None),
                "score": comment.get("score", 0),
                "is_submitter": comment.get("is_submitter", False),
                "parent_id": parent_id,
                "link_id": comment.get("link_id", None),
                "permalink": comment.get("permalink", None),
                "controversiality": comment.get("controversiality", 0),
                "gilded": comment.get("gilded", 0),
            }
        ]

        replies = comment.get("replies", [])
        for reply in replies:
            comment_data.extend(
                self._parse_comment_tree(reply, parent_id=comment.get("id"))
            )

        return comment_data
