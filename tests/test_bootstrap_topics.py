"""Unit tests for scripts/bootstrap_topics.py.

These do not require a running broker: KafkaAdminClient is mocked out,
so they exercise the topic-config translation and the idempotent /
retry logic in isolation.
"""
from unittest.mock import MagicMock, patch

from kafka.errors import TopicAlreadyExistsError

from scripts.bootstrap_topics import TOPICS, NoBrokersAvailable, bootstrap, build_new_topics


def test_build_new_topics_matches_config():
    new_topics = build_new_topics()

    assert len(new_topics) == len(TOPICS)
    names = {t.name for t in new_topics}
    assert names == set(TOPICS.keys())
    for topic in new_topics:
        expected_partitions, expected_replication = TOPICS[topic.name]
        assert topic.num_partitions == expected_partitions
        assert topic.replication_factor == expected_replication


def test_build_new_topics_with_custom_config():
    custom = {"my-topic": (5, 2)}
    new_topics = build_new_topics(custom)

    assert len(new_topics) == 1
    assert new_topics[0].name == "my-topic"
    assert new_topics[0].num_partitions == 5
    assert new_topics[0].replication_factor == 2


@patch("scripts.bootstrap_topics.KafkaAdminClient")
def test_bootstrap_creates_missing_topics(mock_admin_cls):
    mock_admin = MagicMock()
    mock_admin_cls.return_value = mock_admin

    ok = bootstrap(bootstrap_servers="localhost:9092", topics={"test-topic": (1, 1)})

    assert ok is True
    mock_admin.create_topics.assert_called_once()
    mock_admin.close.assert_called_once()


@patch("scripts.bootstrap_topics.KafkaAdminClient")
def test_bootstrap_is_idempotent_when_topic_exists(mock_admin_cls):
    mock_admin = MagicMock()
    mock_admin.create_topics.side_effect = TopicAlreadyExistsError("exists")
    mock_admin_cls.return_value = mock_admin

    ok = bootstrap(bootstrap_servers="localhost:9092", topics={"test-topic": (1, 1)})

    assert ok is True
    mock_admin.close.assert_called_once()


@patch("scripts.bootstrap_topics.time.sleep", return_value=None)
@patch("scripts.bootstrap_topics.KafkaAdminClient")
def test_bootstrap_returns_false_when_broker_never_reachable(mock_admin_cls, mock_sleep):
    mock_admin_cls.side_effect = NoBrokersAvailable()

    ok = bootstrap(bootstrap_servers="localhost:9092", topics={"test-topic": (1, 1)}, max_retries=2, retry_delay_seconds=0)

    assert ok is False
