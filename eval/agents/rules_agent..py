import os


class RulesAgent:
    def __init__(self, rules_dir="rules"):
        self.rules_dir = rules_dir

    def load_rule(self, name: str) -> str:
        path = os.path.join(self.rules_dir, f"{name}.txt")

        if not os.path.exists(path):
            return ""

        with open(path, "r", encoding="utf-8") as f:
            return f.read()

    def get_rules(self, task: str) -> str:
        t = task.lower()

        file_rules = self.load_rule("file")

        if "dashboard" in t or "cross-source" in t or "multiple" in t:
            return file_rules + self.load_rule("multi")

        elif "audit" in t or "integrity" in t or "quality" in t:
            return file_rules + self.load_rule("audit")

        elif ".db" in t or "sqlite" in t or "database" in t:
            return self.load_rule("db")

        elif ".log" in t or "log" in t:
            return file_rules + self.load_rule("log")

        elif "anomaly" in t:
            return file_rules + self.load_rule("anomaly")

        elif "exchange" in t or "currency" in t or "usd" in t:
            return file_rules + self.load_rule("currency")

        else:
            return file_rules