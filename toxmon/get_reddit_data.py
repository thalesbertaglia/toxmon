import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

import praw
from praw.models import Comment, Submission


@dataclass
class RedditDataRetriever:
    subreddit_name: str
    limit: int
    data_dir: str
    praw_ini_path: str
    reddit: praw.Reddit = field(init=False)
    subreddit: praw.models.Subreddit = field(init=False)
    threads_dir: str = field(init=False)
    comments_dir: str = field(init=False)

    def __post_init__(self):
        self._set_praw_config(self.praw_ini_path)
        self.reddit = praw.Reddit("bot1", config_interpolation="basic")
        self.subreddit = self.reddit.subreddit(self.subreddit_name)
        self.threads_dir = os.path.join(self.data_dir, "threads")
        self.comments_dir = os.path.join(self.data_dir, "comments")
        os.makedirs(self.threads_dir, exist_ok=True)
        os.makedirs(self.comments_dir, exist_ok=True)

    @staticmethod
    def _set_praw_config(praw_ini_path: str) -> None:
        """Set the PRAW configuration to use the specified praw.ini directory."""
        os.environ["praw_site"] = "bot1"  # Set the site name to use
        os.environ["praw_ini"] = praw_ini_path  # Specify the praw.ini path

    @staticmethod
    def _serialize_praw_object(obj: Any) -> Any:
        if hasattr(obj, "json_dict"):
            return obj.json_dict()
        elif hasattr(obj, "__dict__"):
            data = obj.__dict__
            return {
                key: RedditDataRetriever._serialize_praw_object(value)
                for key, value in data.items()
                if not key.startswith("_")
            }
        else:
            return str(obj)

    def _process_comment(
        self,
        comment: Comment,
    ) -> Dict[str, Union[str, int, float, List[Any], None]]:
        """Process a single comment to extract relevant information."""
        return {
            "id": comment.id,
            "body": comment.body,
            "author": str(comment.author) if comment.author else None,
            "created_utc": comment.created_utc,
            "score": comment.score,
            "is_submitter": comment.is_submitter,
            "parent_id": comment.parent_id,
            "link_id": comment.link_id,
            "permalink": comment.permalink,
            "controversiality": comment.controversiality,
            "gilded": comment.gilded,
            "likes": comment.likes,
            "num_reports": comment.num_reports
            if hasattr(comment, "num_reports")
            else None,  # num_reports might not be available
            "replies": [self._process_comment(reply) for reply in comment.replies]
            if comment.replies
            else [],
            "saved": comment.saved,
            "score_hidden": comment.score_hidden,
            "stickied": comment.stickied,
            "subreddit_id": comment.subreddit_id,
            "total_awards_received": comment.total_awards_received,
            "upvote_ratio": getattr(
                comment, "upvote_ratio", None
            ),  # upvote_ratio may not always be present
            "depth": comment.depth,
        }

    def _extract_comments(self, submission: Submission) -> List[Dict[str, Any]]:
        """Extract comments from a submission."""
        submission.comments.replace_more(limit=0)  # Ensures no "MoreComments" objects
        return [
            self._process_comment(comment) for comment in submission.comments.list()
        ]

    def _serialize_submission(self, submission: Submission) -> Dict[str, Any]:
        submission_dict = vars(submission)
        serialized_data = {
            key: self._serialize_praw_object(value)
            for key, value in submission_dict.items()
            if not key.startswith("_")
        }
        return serialized_data

    def save_data(
        self, data: Dict[str, Any], filename: str, is_comment: bool = False
    ) -> None:
        """Save data to a JSON file in the specified directory."""
        directory = self.comments_dir if is_comment else self.threads_dir
        file_path = os.path.join(directory, filename)
        with open(file_path, "w", encoding="utf-8") as f:
            json.dump(data, f, ensure_ascii=False, indent=4)

    def retrieve_top_threads(self) -> None:
        for submission in self.subreddit.top(time_filter="all", limit=self.limit):
            # Serialize and save submission information
            submission_data = self._serialize_submission(submission)
            submission_filename = f"{self.subreddit.display_name}_{submission.id}.json"
            self.save_data(submission_data, submission_filename)

            # Serialize and save comments
            comments_data = self._extract_comments(submission)
            comments_filename = (
                f"{self.subreddit.display_name}_{submission.id}_comments.json"
            )
            self.save_data(comments_data, comments_filename, is_comment=True)


if __name__ == "__main__":
    retriever = RedditDataRetriever("youtubedrama", 1, "../data", "../praw.ini")
    retriever.retrieve_top_threads()
