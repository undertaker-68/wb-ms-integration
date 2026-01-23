import json
import os
from datetime import datetime, timedelta

from FBW.app.config_fbw import *
from FBW.app.wb_supplies_client import WBSuppliesClient
from app.ms_client import MSClient


def load_state():
    if not os.path.exists(STATE_FILE):
        return {"last_run": None, "orders": {}}
    with open(STATE_FILE, "r") as f:
        return json.load(f)


def save_state(state):
    with open(STATE_FILE, "w") as f:
        json.dump(state, f, ensure_ascii=False, indent=2)


def main():
    wb = WBSuppliesClient()
    ms = MSClient()

    state = load_state()

    # bootstrap
    if not state["last_run"]:
        state["last_run"] = datetime.utcnow().isoformat()
        save_state(state)
        print("FBW bootstrap done, no data loaded")
        return

    last_run = datetime.fromisoformat(state["last_run"])
    date_from = last_run - timedelta(days=SYNC_LOOKBACK_DAYS)

    supplies = wb.list_supplies(date_from)

    for s in supplies:
        supply_id = s.get("id")
        number = s.get("number")
        status = s.get("statusID")
        plan_date = s.get("supplyDate")

        if not number:
            continue

        order_name = f"fbw-{number}"

        if order_name not in state["orders"]:
            goods = wb.get_goods(supply_id)

            positions = []
            for g in goods:
                article = g.get("article")
                qty = g.get("quantity", 0)
                if not article or qty <= 0:
                    continue

                product = ms.find_product_by_article(article)
                if not product:
                    print(f"❌ product not found: {article}")
                    continue

                positions.append(ms.make_position(product, qty))

            order = ms.create_customerorder(
                name=order_name,
                positions=positions,
                sales_channel_id=MS_SALES_CHANNEL_ID,
                state_id=MS_STATUS_CUSTOMERORDER_ID,
                comment=str(number),
                delivery_planned=plan_date
            )

            state["orders"][order_name] = {
                "order_id": order["id"],
                "move": False,
                "demand": False
            }

        order_id = state["orders"][order_name]["order_id"]

        if plan_date:
            ms.update_customerorder_date(order_id, plan_date)

        # STATUS 3 → MOVE
        if status == 3 and not state["orders"][order_name]["move"]:
            move = ms.create_move(
                name=order_name,
                order_id=order_id,
                source_store=MS_STORE_SOURCE_ID,
                target_store=MS_STORE_WB_ID,
                state_id=MS_STATUS_MOVE_ID
            )
            ms.try_apply(move)
            state["orders"][order_name]["move"] = True

        # STATUS 5 → DEMAND
        if status == 5 and not state["orders"][order_name]["demand"]:
            demand = ms.create_demand(
                name=order_name,
                order_id=order_id,
                store_id=MS_STORE_WB_ID,
                state_id=MS_STATUS_DEMAND_ID
            )
            ms.try_apply(demand)
            state["orders"][order_name]["demand"] = True

    state["last_run"] = datetime.utcnow().isoformat()
    save_state(state)


if __name__ == "__main__":
    main()
