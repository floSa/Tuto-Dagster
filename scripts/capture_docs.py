"""Regenere les captures de documentation/images en rejouant le tutoriel.

Lance une instance Dagster jetable (.docs_capture/), execute le parcours de
TUTORIAL.md sur un vrai pipeline, photographie chaque ecran, puis arrete tout.
Les commandes CLI et leur sortie brute sont gardees dans .docs_capture/cli/.

    uv run --with playwright --with pillow python scripts/capture_docs.py

Navigateur requis une seule fois :

    uv run --with playwright playwright install chromium
"""

import json
import os
import re
import shutil
import signal
import subprocess
import time
import urllib.request
from io import BytesIO
from pathlib import Path

from PIL import Image
from playwright.sync_api import Browser, Locator, Page, expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".docs_capture"
IMAGES = ROOT / "documentation" / "images"
PORT = 3113
BASE = f"http://127.0.0.1:{PORT}"
ENV = {
    **{k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"},
    "DAGSTER_HOME": str(WORK / "dagster_home"),
    "STREAMIFY_DATA_DIR": str(WORK / "data"),
    "NO_COLOR": "1",
}
UI_PARTITION = "2024-01-15"
CLI_PARTITION = "2024-01-16"
BAD_PARTITION = "2024-03-15"
BAD_PARTITION_UI = "2024-03-20"
BAD_CONFIG_YAML = """ops:
  raw_tracks_data:
    config:
      null_track_ids: 3
"""
BACKFILL_RANGE = "[2024-02-01...2024-03-01]"
ACCENT = "#e5484d"


def cli(name: str, *args: str) -> int:
    """Lance une commande dagster dans l'instance jetable et archive sa sortie."""
    command = ["uv", "run", "--frozen", "dagster", *args]
    done = subprocess.run(
        command, cwd=ROOT, env=ENV, capture_output=True, text=True, check=False
    )
    log = WORK / "cli" / f"{name}.log"
    output = re.sub(r"\x1b\[[0-9;]*m", "", done.stdout + done.stderr)
    log.write_text(f"$ dagster {' '.join(args)}\n{output}\nexit={done.returncode}\n")
    print(f"[cli] {name}: exit={done.returncode}")
    return done.returncode


def wait_for_server(timeout: int = 120) -> None:
    deadline = time.time() + timeout
    while time.time() < deadline:
        try:
            urllib.request.urlopen(f"{BASE}/server_info", timeout=2)
            return
        except OSError:
            time.sleep(1)
    raise TimeoutError(f"Dagster n'a pas demarre sur {BASE}")


def graphql(query: str) -> dict:
    request = urllib.request.Request(
        f"{BASE}/graphql",
        data=json.dumps({"query": query}).encode(),
        headers={"Content-Type": "application/json"},
    )
    with urllib.request.urlopen(request, timeout=30) as response:
        return json.load(response)["data"]


def latest_run_id(partition: str) -> str:
    """Identifiant du run le plus recent d'une partition."""
    query = (
        '{runsOrError(filter:{tags:[{key:"dagster/partition",value:"%s"}]},limit:1)'
        "{... on Runs{results{runId}}}}" % partition
    )
    return graphql(query)["runsOrError"]["results"][0]["runId"]


def wait_run_finished(run_id: str, timeout: int = 180) -> str:
    deadline = time.time() + timeout
    while time.time() < deadline:
        status = graphql('{runOrError(runId:"%s"){... on Run{status}}}' % run_id)[
            "runOrError"
        ]["status"]
        if status in ("SUCCESS", "FAILURE", "CANCELED"):
            return status
        time.sleep(1)
    raise TimeoutError(f"Run {run_id} non termine")


def backfill_state() -> tuple[str, str, int, int]:
    """(id, statut, runs termines, runs totaux) du dernier backfill."""
    query = (
        "{partitionBackfillsOrError(limit:1){... on PartitionBackfills{results"
        "{id status numPartitions assetBackfillData{assetBackfillStatuses"
        "{... on AssetPartitionsStatusCounts{numPartitionsTargeted "
        "numPartitionsMaterialized}}}}}}}"
    )
    backfill = graphql(query)["partitionBackfillsOrError"]["results"][0]
    counts = backfill["assetBackfillData"]["assetBackfillStatuses"]
    done = sum(c["numPartitionsMaterialized"] for c in counts)
    total = sum(c["numPartitionsTargeted"] for c in counts)
    return backfill["id"], backfill["status"], done, total


def above(target: Locator) -> tuple[Locator, bool]:
    """Repere pose au-dessus de la cible, quand un voisin serait masque a gauche."""
    return target, True


class Camera:
    """Navigation + captures annotees de reperes numerotes."""

    def __init__(self, page: Page) -> None:
        self.page = page

    def goto(self, path: str, settle: int = 3000) -> None:
        self.page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
        self.page.wait_for_timeout(settle)

    def _annotate(self, targets: list) -> None:
        for number, item in enumerate(targets, 1):
            target, on_top = item if isinstance(item, tuple) else (item, False)
            box = target.bounding_box()
            if box is None:
                raise RuntimeError(f"Repere {number} invisible: {target}")
            self.page.evaluate(
                """([box, number, color, onTop]) => {
                    const frame = document.createElement('div');
                    frame.className = 'doc-mark';
                    frame.style.cssText = `position:fixed;z-index:99999;
                        left:${box.x - 4}px;top:${box.y - 4}px;
                        width:${box.width + 8}px;height:${box.height + 8}px;
                        border:2px solid ${color};border-radius:6px;
                        pointer-events:none;box-sizing:border-box`;
                    const badge = document.createElement('div');
                    badge.textContent = number;
                    const side = box.x < 40 ? `right:-30px` : `left:-30px`;
                    const place = onTop
                        ? `left:calc(50% - 12px);top:-28px`
                        : `${side};top:calc(50% - 12px)`;
                    badge.style.cssText = `position:absolute;${place};
                        width:24px;height:24px;border-radius:50%;background:${color};
                        color:#fff;font:bold 13px/24px sans-serif;text-align:center`;
                    frame.appendChild(badge);
                    document.body.appendChild(frame);
                }""",
                [box, number, ACCENT, on_top],
            )

    def shot(self, name: str, *targets) -> None:
        self.page.mouse.move(1439, 899)
        self.page.wait_for_timeout(300)
        self._annotate(list(targets))
        self.page.screenshot(path=IMAGES / f"{name}.png")
        self.page.evaluate(
            "document.querySelectorAll('.doc-mark').forEach(e => e.remove())"
        )
        print(f"[png] {name}")

    def button(self, name: str) -> Locator:
        return self.page.get_by_role("button", name=name).first

    def text(self, value: str) -> Locator:
        return self.page.get_by_text(value, exact=True).first

    def open_materialize(self, partitions: str) -> Locator:
        """Ouvre 'Materialize all' et saisit les partitions; renvoie le lancement."""
        self.goto("/asset-groups")
        self.button("Materialize all").click()
        dialog = self.page.get_by_role("dialog")
        dialog.get_by_role("button", name="Custom").click()
        field = dialog.get_by_placeholder(re.compile("^ex:"))
        field.fill(partitions)
        field.press("Enter")
        self.page.wait_for_timeout(1000)
        return dialog.get_by_role("button", name=re.compile("^Launch"))


def lineage_closeup(browser: Browser, name: str) -> None:
    """Gros plan haute resolution du graphe: illisible en pleine page."""
    context = browser.new_context(
        viewport={"width": 1440, "height": 900},
        device_scale_factor=2,
        locale="fr-FR",
        timezone_id="Europe/Paris",
        color_scheme="light",
    )
    page = context.new_page()
    page.goto(f"{BASE}/asset-groups", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(4000)
    skip = page.get_by_role("button", name="Skip")
    if skip.count():
        skip.click()
        page.wait_for_timeout(800)
    first = page.get_by_text("raw_tracks_data", exact=True).last.bounding_box()
    last = page.get_by_text("daily_artist_stats", exact=True).last.bounding_box()
    page.screenshot(
        path=IMAGES / f"{name}.png",
        clip={
            "x": first["x"] - 30,
            "y": first["y"] - 20,
            "width": last["x"] - first["x"] + 260,
            "height": 130,
        },
    )
    context.close()
    print(f"[png] {name}")


def tour_interface(cam: Camera) -> None:
    cam.goto("/asset-groups")
    skip = cam.page.get_by_role("button", name="Skip")
    expect(skip).to_be_visible(timeout=20000)
    cam.shot("01-fenetre-accueil", skip)
    skip.click()
    cam.page.wait_for_timeout(1500)
    cam.shot(
        "02-lignage-vide",
        cam.page.locator("a[href='/asset-groups'], a[href^='/asset-groups']").first,
        cam.button("Materialize all"),
    )
    lineage_closeup(cam.page.context.browser, "02b-lignage-gros-plan")
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot(
        "03-check-jamais-execute",
        above(cam.page.get_by_role("tab", name="Checks").first),
        cam.text("clean_tracks_id_not_null").locator("xpath=.."),
    )


def materialize_from_ui(cam: Camera) -> None:
    launch = cam.open_materialize(UI_PARTITION)
    dialog = cam.page.get_by_role("dialog")
    cam.shot(
        "04-materialiser-depuis-interface",
        above(dialog.get_by_role("button", name="Custom")),
        dialog.get_by_placeholder(re.compile("^ex:")),
        above(launch),
    )
    launch.click()
    cam.page.wait_for_timeout(2500)
    run_id = latest_run_id(UI_PARTITION)
    wait_run_finished(run_id)
    cam.goto(f"/runs/{run_id}", settle=4000)
    cam.shot(
        "05-run-termine",
        cam.page.get_by_text("Success", exact=False).first,
        cam.page.get_by_text("RUN_SUCCESS", exact=False).first,
    )


def materialize_from_cli(cam: Camera) -> None:
    code = cli(
        "materialize",
        "asset",
        "materialize",
        "-m",
        "streamify_ops",
        "--select",
        "*",
        "--partition",
        CLI_PARTITION,
    )
    assert code == 0
    cam.goto("/runs")
    cam.shot(
        "06-runs-interface-et-cli",
        cam.text(f"partition: {CLI_PARTITION}"),
        cam.text(f"partition: {UI_PARTITION}"),
    )
    cam.goto(f"/assets/daily_artist_stats?view=partitions&partition={CLI_PARTITION}")
    cam.shot(
        "07-partitions-apres-cli",
        above(cam.page.get_by_text(re.compile(r"^Materialized \(")).first),
        cam.page.get_by_text("View all historical events").first,
    )
    cam.goto("/assets/daily_artist_stats?view=events")
    cam.shot(
        "08-metadonnees-gold",
        cam.text("preview"),
        cam.text("path"),
    )
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot("09-check-reussi", cam.page.get_by_text("Passed").last)


def run_backfill(cam: Camera) -> None:
    launch = cam.open_materialize(BACKFILL_RANGE)
    dialog = cam.page.get_by_role("dialog")
    cam.shot(
        "10-backfill-dialogue",
        dialog.get_by_placeholder(re.compile("^ex:")),
        launch,
    )
    launch.click()
    cam.page.wait_for_timeout(4000)
    backfill_id, *_ = backfill_state()
    cam.goto(f"/runs/b/{backfill_id}")
    frames: list[Image.Image] = []
    shot_mid = False
    started = time.time()
    while True:
        _, status, done, total = backfill_state()
        png = cam.page.screenshot()
        frames.append(Image.open(BytesIO(png)).convert("RGB").resize((960, 600)))
        if not shot_mid and total and done >= total / 3:
            cam.shot("11-backfill-en-cours")
            shot_mid = True
        if status != "REQUESTED":
            break
        if time.time() - started > 900:
            raise TimeoutError("Backfill non termine")
        cam.page.wait_for_timeout(4000)
    print(f"[backfill] {status} {done}/{total} en {time.time() - started:.0f}s")
    cam.goto(f"/runs/b/{backfill_id}", settle=4000)
    cam.shot("12-backfill-termine")
    frames.append(
        Image.open(BytesIO(cam.page.screenshot())).convert("RGB").resize((960, 600))
    )
    frames = frames[:: max(1, len(frames) // 25)]
    frames[0].save(
        IMAGES / "backfill.gif",
        save_all=True,
        append_images=[f.quantize(64) for f in frames[1:]],
        duration=450,
        loop=0,
        optimize=True,
    )
    cam.goto("/assets/daily_artist_stats?view=partitions")
    cam.shot(
        "13-partitions-apres-backfill",
        above(cam.page.get_by_text(re.compile(r"^Materialized \(")).first),
    )
    lineage_closeup(cam.page.context.browser, "14-lignage-apres-backfill")
    cam.goto("/assets/clean_tracks?view=overview", settle=4000)
    plots = cam.button("Plots")
    plots.click()
    cam.page.wait_for_timeout(1000)
    events = cam.page.locator("button", has_text="Events").last
    events.click()
    cam.page.wait_for_timeout(3000)
    cam.shot("15-courbe-rows", above(events), above(plots))


def break_data_quality(cam: Camera) -> None:
    config = {"ops": {"raw_tracks_data": {"config": {"null_track_ids": 3}}}}
    code = cli(
        "materialize-echec",
        "asset",
        "materialize",
        "-m",
        "streamify_ops",
        "--select",
        "*",
        "--partition",
        BAD_PARTITION,
        "--config-json",
        json.dumps(config),
    )
    assert code == 1
    run_id = latest_run_id(BAD_PARTITION)
    cam.goto(f"/runs/{run_id}", settle=4000)
    cam.shot(
        "16-run-bloque",
        cam.page.get_by_text("Failure", exact=False).first,
        cam.page.get_by_text("Not executing", exact=False).first,
    )
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot(
        "17-check-en-echec",
        cam.page.get_by_text("Failed").last,
        cam.text("null_count"),
    )
    cam.goto(f"/assets/clean_tracks?view=partitions&partition={BAD_PARTITION}")
    cam.shot(
        "18-silver-ecrite-malgre-echec",
        above(cam.page.get_by_text("Materialized", exact=True).first),
    )
    cam.goto(f"/assets/daily_artist_stats?view=partitions&partition={BAD_PARTITION}")
    cam.shot(
        "19-gold-non-produite",
        cam.page.get_by_text("did not materialize", exact=False).first,
        above(cam.page.get_by_text(re.compile(r"^Failed \(")).first),
    )

    launch = cam.open_materialize(BAD_PARTITION_UI)
    dialog = cam.page.get_by_role("dialog").first
    dialog.get_by_text("Config", exact=True).click()
    cam.page.wait_for_timeout(600)
    cam.button("Add config").click()
    cam.page.wait_for_timeout(2500)
    cam.page.locator(".CodeMirror").last.click()
    cam.page.keyboard.insert_text(BAD_CONFIG_YAML)
    cam.page.wait_for_timeout(1500)
    save = cam.button("Save config")
    cam.shot("20-config-dans-interface", save)
    save.click()
    cam.page.wait_for_timeout(1000)
    launch.click()
    cam.page.wait_for_timeout(2500)
    status = wait_run_finished(latest_run_id(BAD_PARTITION_UI))
    assert status == "FAILURE", status

    for partition in (BAD_PARTITION, BAD_PARTITION_UI):
        code = cli(
            f"materialize-reprise-{partition}",
            "asset",
            "materialize",
            "-m",
            "streamify_ops",
            "--select",
            "*",
            "--partition",
            partition,
        )
        assert code == 0
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot("21-check-repare", cam.page.get_by_text("Passed").last)


def schedule_tour(cam: Camera) -> None:
    schedule = "/locations/streamify_ops/schedules/daily_streamify_schedule"
    cli("schedule-list-avant", "schedule", "list", "-m", "streamify_ops")
    cam.goto(schedule)
    switch = cam.page.locator("input[type=checkbox]").first.locator("xpath=..")
    cam.shot("22-schedule-arrete", switch)
    cli(
        "schedule-start",
        "schedule",
        "start",
        "-m",
        "streamify_ops",
        "daily_streamify_schedule",
    )
    cli("schedule-list-apres", "schedule", "list", "-m", "streamify_ops")
    cam.goto(schedule)
    switch = cam.page.locator("input[type=checkbox]").first.locator("xpath=..")
    cam.shot("23-schedule-actif", switch)
    cam.button("Preview tick result").click()
    cam.page.wait_for_timeout(2000)
    cam.button("Continue").click()
    cam.page.wait_for_timeout(5000)
    cam.shot(
        "24-schedule-apercu-tick",
        cam.page.get_by_text("run requests", exact=False).first,
        cam.page.get_by_role("dialog").get_by_text("daily_streamify_job").first,
    )
    cli(
        "schedule-stop",
        "schedule",
        "stop",
        "-m",
        "streamify_ops",
        "daily_streamify_schedule",
    )


def main() -> None:
    shutil.rmtree(WORK, ignore_errors=True)
    (WORK / "dagster_home").mkdir(parents=True)
    (WORK / "cli").mkdir()
    shutil.rmtree(IMAGES, ignore_errors=True)
    IMAGES.mkdir(parents=True)

    server = subprocess.Popen(
        [
            "uv",
            "run",
            "--frozen",
            "dagster",
            "dev",
            "-m",
            "streamify_ops",
            "-p",
            str(PORT),
            "-h",
            "127.0.0.1",
        ],
        cwd=ROOT,
        env=ENV,
        stdout=(WORK / "server.log").open("w"),
        stderr=subprocess.STDOUT,
        start_new_session=True,
    )
    try:
        wait_for_server()
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            page = browser.new_context(
                viewport={"width": 1440, "height": 900},
                locale="fr-FR",
                timezone_id="Europe/Paris",
                color_scheme="light",
            ).new_page()
            cam = Camera(page)
            tour_interface(cam)
            materialize_from_ui(cam)
            materialize_from_cli(cam)
            run_backfill(cam)
            break_data_quality(cam)
            schedule_tour(cam)
            browser.close()
    finally:
        os.killpg(server.pid, signal.SIGTERM)
        server.wait(timeout=30)
    print(f"Captures dans {IMAGES}")


if __name__ == "__main__":
    main()
