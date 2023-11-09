import ast
import re
from collections import Counter
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
        num_comments: int = thread_json.get("num_comments", 0)
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
            "num_comments": num_comments,
            "ups": ups,
            "downs": downs,
            "upvote_ratio": upvote_ratio,
            "score": score,
            "selftext": selftext,
            "media": media,
            "media_only": media_only,
            "created_utc": created_utc,
            "youtube_media": self.extract_youtube_media(thread_json),
            "urls": self.extract_urls_from_selftext(thread_json),
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
                "author_name": comment.get("author", None),
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

    def extract_youtube_media(self, parsed_thread: Dict[str, Any]) -> str:
        """
        Extracts YouTube media information from the provided data.
        """
        media_str = parsed_thread.get("media")
        if media_str == "None":
            return "None"
        else:
            try:
                media_dict = ast.literal_eval(media_str)
                if "type" in media_dict and media_dict["type"] == "youtube.com":
                    return media_dict["oembed"]["author_name"]
            except ValueError:
                print(f"Error parsing media: {media_str}")
                return "ERROR"

    def extract_urls_from_selftext(self, parsed_data: Dict[str, Any]) -> List[str]:
        """
        Extracts URLs from the 'selftext' field of the provided data.
        """
        urls = self.url_pattern.findall(parsed_data.get("selftext"))
        return urls
