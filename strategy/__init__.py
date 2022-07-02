from .console_strategy import ConsoleStrategy
from .event_hub_strategy import EventHubStrategy


def get_strategy_obj(strategy_name, dataset):
    strategies = {
        "console": ConsoleStrategy,
        "event_hub": EventHubStrategy,
    }
    return strategies[strategy_name](dataset)
