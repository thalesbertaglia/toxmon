import json
import os
from dataclasses import dataclass, field
from typing import Any, Dict, List, Union

import praw
from praw.models import Comment, Submission


@dataclass
class RedditDataRetriever:
    data_dir: str
    reddit: praw.Reddit = field(init=False)
    threads_dir: str = field(init=False)
    comments_dir: str = field(init=False)

    def __post_init__(self):
        self.reddit = praw.Reddit("bot1", config_interpolation="basic")
        self.threads_dir = os.path.join(self.data_dir, "threads")
        self.comments_dir = os.path.join(self.data_dir, "comments")
        os.makedirs(self.threads_dir, exist_ok=True)
        os.makedirs(self.comments_dir, exist_ok=True)

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

    def retrieve_top_threads(
        self, subreddit_name: str, limit: int = 10, log_progress: bool = True
    ) -> None:
        subreddit = self.reddit.subreddit(subreddit_name)
        for i, submission in enumerate(subreddit.top(time_filter="all", limit=limit)):
            if log_progress:
                print(f"Processing thread {i + 1} of {limit}...")
            # Serialize and save submission information
            submission_data = self._serialize_submission(submission)
            submission_filename = f"{subreddit.display_name}_{submission.id}.json"
            self.save_data(submission_data, submission_filename)

            # Serialize and save comments
            comments_data = self._extract_comments(submission)
            comments_filename = (
                f"{subreddit.display_name}_{submission.id}_comments.json"
            )
            self.save_data(comments_data, comments_filename, is_comment=True)


if __name__ == "__main__":
    retriever = RedditDataRetriever("../data")
    retriever.retrieve_top_threads("youtubedrama", 1)
