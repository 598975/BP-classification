import sqlite3
from typing import Any
from homeassistant.components.blueprint import Blueprint

def fetch_blueprint_code() -> list[Any]:
    conn = sqlite3.connect("home_assistant_blueprints.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT id, blueprint_code FROM blueprints")
    blueprints = cursor.fetchall()
    conn.close()
    return blueprints

def delete_blueprints(blueprint_ids: list[int]) -> None:
    conn = sqlite3.connect("home_assistant_blueprints.sqlite")
    cursor = conn.cursor()
    cursor.executemany("DELETE FROM blueprints WHERE id = ?", [(bp_id,) for bp_id in blueprint_ids])
    conn.commit()
    conn.close()
    
def get_all_blueprints() -> list[Any]:
    conn = sqlite3.connect("home_assistant_blueprints.sqlite")
    cursor = conn.cursor()
    cursor.execute("SELECT * FROM blueprints")
    blueprints = cursor.fetchall()
    conn.close()
    return blueprints