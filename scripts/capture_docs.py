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
from datetime import datetime, timedelta, timezone
from io import BytesIO
from pathlib import Path

from PIL import Image
from playwright.sync_api import Browser, Locator, Page, expect, sync_playwright

ROOT = Path(__file__).resolve().parents[1]
WORK = ROOT / ".docs_capture"
DATA = WORK / "data"
HOME = WORK / "dagster_home"
IMAGES = ROOT / "documentation" / "images"
PORT = 3113
BASE = f"http://127.0.0.1:{PORT}"
MODULE = "streamify_ops.definitions"
ENV = {
    **{k: v for k, v in os.environ.items() if k != "VIRTUAL_ENV"},
    "DAGSTER_HOME": str(HOME),
    "STREAMIFY_DATA_DIR": str(DATA),
    "NO_COLOR": "1",
}
UI_PARTITION = "2024-01-15"
CLI_PARTITION = "2024-01-16"
BAD_PARTITION = "2024-03-15"
BAD_PARTITION_UI = "2024-03-20"
INBOX_PARTITION = "2024-04-10"
ALERT_PARTITION = "2024-04-20"
RETRY_PARTITION = "2024-05-01"
WEEKLY_PARTITION = "2024-02-20"
QUEUE_PARTITIONS = [f"2024-06-{day:02d}" for day in range(1, 7)]
ALL_ASSETS = "*"
BAD_CONFIG = {"ops": {"raw_tracks_data": {"config": {"null_track_ids": 3}}}}
BAD_CONFIG_YAML = """ops:
  raw_tracks_data:
    config:
      null_track_ids: 3
"""
RETRY_CONFIG = {"ops": {"raw_tracks_data": {"config": {"fail_attempts": 1}}}}
EXHAUSTED_CONFIG = {"ops": {"raw_tracks_data": {"config": {"fail_attempts": 3}}}}
BACKFILL_RANGE = "[2024-02-01...2024-03-01]"
AUTOMATION_SENSOR = "default_automation_condition_sensor"
WEEKLY_SOURCE = ROOT / "src/streamify_ops/optional/weekly_artist_stats.py"
WEEKLY_ACTIVE = ROOT / "src/streamify_ops/defs/assets/weekly_artist_stats.py"
ACCENT = "#e5484d"


def latest_partition() -> str:
    """La derniere partition complete: la veille, en UTC."""
    return (datetime.now(timezone.utc) - timedelta(days=1)).strftime("%Y-%m-%d")


def _command(name: str, command: list[str]) -> int:
    """Lance une commande dans l'instance jetable et archive sa sortie."""
    done = subprocess.run(
        ["uv", "run", "--frozen", *command],
        cwd=ROOT,
        env=ENV,
        capture_output=True,
        text=True,
        check=False,
    )
    log = WORK / "cli" / f"{name}.log"
    output = re.sub(r"\x1b\[[0-9;]*m", "", done.stdout + done.stderr)
    log.write_text(f"$ {' '.join(command)}\n{output}\nexit={done.returncode}\n")
    print(f"[cli] {name}: exit={done.returncode}", flush=True)
    return done.returncode


def launch(name: str, assets: str, partition: str, config: dict | None = None) -> int:
    """dg launch sur une selection d'assets et une partition."""
    command = ["dg", "launch", "--assets", assets, "--partition", partition]
    if config:
        command += ["--config-json", json.dumps(config)]
    return _command(name, command)


def instigator(name: str, kind: str, action: str, target: str) -> int:
    """dagster schedule|sensor start|stop sur l'instance jetable."""
    return _command(name, ["dagster", kind, action, "-m", MODULE, target])


class Server:
    """Le serveur `dg dev`, redemarrable avec un environnement different."""

    def __init__(self) -> None:
        self.process: subprocess.Popen | None = None

    def start(self, **extra_env: str) -> None:
        self.process = subprocess.Popen(
            [
                "uv",
                "run",
                "--frozen",
                "dg",
                "dev",
                "--port",
                str(PORT),
                "--host",
                "127.0.0.1",
            ],
            cwd=ROOT,
            env={**ENV, **extra_env},
            stdout=(WORK / "server.log").open("a"),
            stderr=subprocess.STDOUT,
            start_new_session=True,
        )
        wait_for_server()

    def stop(self) -> None:
        if self.process is None:
            return
        os.killpg(self.process.pid, signal.SIGTERM)
        self.process.wait(timeout=60)
        self.process = None
        time.sleep(3)


def wait_for_server(timeout: int = 180) -> None:
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


def runs_with_tag(key: str, value: str, limit: int = 20) -> list[dict]:
    """Runs portant ce tag, du plus recent au plus ancien."""
    query = (
        '{runsOrError(filter:{tags:[{key:"%s",value:"%s"}]},limit:%d)'
        "{... on Runs{results{runId status assetSelection{path}}}}}"
        % (key, value, limit)
    )
    return graphql(query)["runsOrError"]["results"]


def latest_run_id(partition: str) -> str:
    """Identifiant du run le plus recent d'une partition."""
    return runs_with_tag("dagster/partition", partition, 1)[0]["runId"]


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


def wait_until(check, what: str, timeout: int = 300, every: float = 2):
    """Attend que `check()` renvoie une valeur vraie, et la renvoie."""
    deadline = time.time() + timeout
    while time.time() < deadline:
        value = check()
        if value:
            return value
        time.sleep(every)
    raise TimeoutError(f"Attente depassee: {what}")


def freshness() -> str:
    query = (
        '{assetNodes(assetKeys:[{path:["daily_artist_stats"]}])'
        "{freshnessStatusInfo{freshnessStatus}}}"
    )
    node = graphql(query)["assetNodes"][0]
    return node["freshnessStatusInfo"]["freshnessStatus"]


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


def above(target: Locator) -> tuple[Locator, str]:
    """Repere pose au-dessus de la cible, quand un voisin serait masque a gauche."""
    return target, "top"


def below(target: Locator) -> tuple[Locator, str]:
    """Repere pose sous la cible, quand la place manque a gauche et au-dessus."""
    return target, "bottom"


def new_context(browser: Browser, scale: int = 2):
    """Contexte double resolution: les captures restent nettes une fois affichees."""
    return browser.new_context(
        viewport={"width": 1440, "height": 900},
        device_scale_factor=scale,
        locale="fr-FR",
        timezone_id="Europe/Paris",
        color_scheme="light",
    )


def gif_frame(png: bytes) -> Image.Image:
    return Image.open(BytesIO(png)).convert("RGB").resize((1200, 750), Image.LANCZOS)


def expand_groups(page: Page) -> Locator:
    """Ouvre le menu du graphe; renvoie l'entree qui deplie les groupes."""
    page.mouse.move(1413, 792)
    page.mouse.click(1413, 792)
    page.wait_for_timeout(800)
    return page.get_by_text("Expand all groups").locator("visible=true").first


class Camera:
    """Navigation + captures annotees de reperes numerotes."""

    def __init__(self, page: Page) -> None:
        self.page = page

    def goto(self, path: str, settle: int = 3500) -> None:
        self.page.goto(f"{BASE}{path}", wait_until="domcontentloaded", timeout=60000)
        self.page.wait_for_timeout(settle)

    def _annotate(self, targets: list) -> None:
        for number, item in enumerate(targets, 1):
            target, where = item if isinstance(item, tuple) else (item, "side")
            box = target.bounding_box()
            if box is None:
                raise RuntimeError(f"Repere {number} invisible: {target}")
            self.page.evaluate(
                """([box, number, color, where]) => {
                    const frame = document.createElement('div');
                    frame.className = 'doc-mark';
                    frame.style.cssText = `position:fixed;z-index:99999;
                        left:${box.x - 6}px;top:${box.y - 5}px;
                        width:${box.width + 12}px;height:${box.height + 10}px;
                        border:2px solid ${color};border-radius:7px;
                        pointer-events:none;box-sizing:border-box`;
                    const badge = document.createElement('div');
                    badge.textContent = number;
                    const side = box.x < 40 ? `right:-32px` : `left:-32px`;
                    const place = {
                        top: `left:calc(50% - 12px);top:-30px`,
                        bottom: `left:calc(50% - 12px);bottom:-30px`,
                        side: `${side};top:calc(50% - 12px)`,
                    }[where];
                    badge.style.cssText = `position:absolute;${place};
                        width:24px;height:24px;border-radius:50%;background:${color};
                        color:#fff;font:bold 13px/24px sans-serif;text-align:center;
                        box-shadow:0 0 0 2px #fff`;
                    frame.appendChild(badge);
                    document.body.appendChild(frame);
                }""",
                [box, number, ACCENT, where],
            )

    def shot(self, name: str, *targets) -> None:
        self.page.mouse.move(1439, 899)
        self.page.wait_for_timeout(400)
        self._annotate(list(targets))
        self.page.screenshot(path=IMAGES / f"{name}.png")
        self.page.evaluate(
            "document.querySelectorAll('.doc-mark').forEach(e => e.remove())"
        )
        print(f"[png] {name}", flush=True)

    def button(self, name: str) -> Locator:
        return self.page.get_by_role("button", name=name).first

    def text(self, value: str) -> Locator:
        return self.page.get_by_text(value, exact=True).first

    def show_all_partitions(self) -> Locator:
        """Ouvre 'Materialize all'; renvoie le lien qui leve la limite de 90 jours."""
        self.goto("/asset-groups")
        self.button("Materialize all").click()
        self.page.wait_for_timeout(1500)
        return self.page.get_by_role("dialog").get_by_text("Show all partitions")

    def open_materialize(self, partitions: str) -> Locator:
        """Ouvre 'Materialize all' et saisit les partitions; renvoie le lancement."""
        show_all = self.show_all_partitions()
        if show_all.count():
            show_all.click()
            self.page.wait_for_timeout(1000)
        dialog = self.page.get_by_role("dialog")
        dialog.get_by_role("button", name="Custom").click()
        field = dialog.get_by_placeholder(re.compile("^ex:"))
        field.fill(partitions)
        field.press("Enter")
        self.page.wait_for_timeout(1000)
        return dialog.get_by_role("button", name=re.compile("^Launch"))

    def reload_definitions(self) -> None:
        self.goto("/asset-groups")
        self.button("Reload definitions").click()
        self.page.wait_for_timeout(6000)


def lineage_closeup(browser: Browser, name: str) -> None:
    """Gros plan tres haute resolution du graphe: illisible en pleine page."""
    context = new_context(browser, scale=3)
    page = context.new_page()
    page.goto(f"{BASE}/asset-groups", wait_until="domcontentloaded", timeout=60000)
    page.wait_for_timeout(4000)
    skip = page.get_by_role("button", name="Skip")
    if skip.count():
        skip.click()
        page.wait_for_timeout(800)
    expand_groups(page).click()
    page.wait_for_timeout(1500)
    page.reload(wait_until="domcontentloaded")
    page.wait_for_timeout(4000)
    first = page.get_by_text("raw_tracks_data", exact=True).last.bounding_box()
    last = page.get_by_text("daily_artist_stats", exact=True).last.bounding_box()
    page.screenshot(
        path=IMAGES / f"{name}.png",
        clip={
            "x": first["x"] - 40,
            "y": first["y"] - 72,
            "width": last["x"] - first["x"] + 250,
            "height": 215,
        },
    )
    context.close()
    print(f"[png] {name}", flush=True)


def tour_interface(cam: Camera) -> None:
    cam.goto("/asset-groups")
    skip = cam.page.get_by_role("button", name="Skip")
    expect(skip).to_be_visible(timeout=20000)
    cam.shot("01-fenetre-accueil", skip)
    skip.click()
    cam.page.wait_for_timeout(1500)
    cam.shot(
        "02-lignage-groupes",
        cam.page.locator("a[href='/asset-groups'], a[href^='/asset-groups']").first,
        cam.button("Materialize all"),
    )
    entry = expand_groups(cam.page)
    cam.shot("02a-deplier-les-groupes", entry)
    entry.click()
    cam.page.wait_for_timeout(1500)
    cam.goto("/asset-groups", settle=4000)
    cam.shot("02-lignage-vide")
    lineage_closeup(cam.page.context.browser, "02b-lignage-gros-plan")
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot(
        "03-check-jamais-execute",
        above(cam.page.get_by_role("tab", name="Checks").first),
        cam.text("clean_tracks_id_not_null").locator("xpath=.."),
    )


def materialize_from_ui(cam: Camera) -> None:
    show_all = cam.show_all_partitions()
    cam.shot("04a-afficher-toutes-les-partitions", above(show_all))
    launch_button = cam.open_materialize(UI_PARTITION)
    dialog = cam.page.get_by_role("dialog")
    cam.shot(
        "04-materialiser-depuis-interface",
        above(dialog.get_by_role("button", name="Custom")),
        dialog.get_by_placeholder(re.compile("^ex:")),
        above(launch_button),
    )
    launch_button.click()
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
    assert launch("launch", ALL_ASSETS, CLI_PARTITION) == 0
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
        cam.text("dagster/uri"),
        cam.text("dagster/row_count"),
        cam.text("dagster/column_schema"),
    )
    cam.goto("/assets/daily_artist_stats?view=overview")
    cam.shot(
        "08b-vue-ensemble-asset",
        above(cam.text("Row count")),
        cam.text("Columns"),
        cam.text("Owners"),
        cam.text("Kinds"),
    )
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot("09-check-reussi", cam.page.get_by_text("Passed").last)


def run_backfill(cam: Camera) -> None:
    launch_button = cam.open_materialize(BACKFILL_RANGE)
    dialog = cam.page.get_by_role("dialog")
    cam.shot(
        "10-backfill-dialogue",
        dialog.get_by_placeholder(re.compile("^ex:")),
        above(launch_button),
    )
    launch_button.click()
    cam.page.wait_for_timeout(4000)
    backfill_id, *_ = backfill_state()
    cam.goto(f"/runs/b/{backfill_id}")
    frames: list[Image.Image] = []
    shot_mid = False
    started = time.time()
    while True:
        _, status, done, total = backfill_state()
        frames.append(gif_frame(cam.page.screenshot()))
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
    frames.append(gif_frame(cam.page.screenshot()))
    frames = frames[:: max(1, len(frames) // 25)]
    frames[0].save(
        IMAGES / "backfill.gif",
        save_all=True,
        append_images=[f.quantize(128) for f in frames[1:]],
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
    events.evaluate("e => e.scrollIntoView({block: 'start'})")
    cam.page.wait_for_timeout(1000)
    cam.shot("15-courbe-rows", below(events), below(plots))


def break_data_quality(cam: Camera) -> None:
    assert launch("launch-echec", ALL_ASSETS, BAD_PARTITION, BAD_CONFIG) == 1
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
        below(cam.text(BAD_PARTITION)),
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

    launch_button = cam.open_materialize(BAD_PARTITION_UI)
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
    launch_button.click()
    cam.page.wait_for_timeout(2500)
    status = wait_run_finished(latest_run_id(BAD_PARTITION_UI))
    assert status == "FAILURE", status

    assert launch(f"launch-reprise-{BAD_PARTITION}", ALL_ASSETS, BAD_PARTITION) == 0
    cam.goto("/assets/clean_tracks?view=checks")
    partitions = cam.page.get_by_text("Partitions", exact=True).last
    partitions.click()
    cam.page.wait_for_timeout(3000)
    cam.page.get_by_placeholder("Filter by partition").fill(BAD_PARTITION_UI)
    cam.page.wait_for_timeout(1000)
    row = cam.text(BAD_PARTITION_UI)
    row.click()
    cam.page.wait_for_timeout(2500)
    cam.shot(
        "20b-check-par-partition",
        above(partitions),
        above(cam.page.get_by_text(re.compile(r"^Failed")).last),
        cam.text("null_count"),
    )
    assert (
        launch(f"launch-reprise-{BAD_PARTITION_UI}", ALL_ASSETS, BAD_PARTITION_UI) == 0
    )
    cam.goto("/assets/clean_tracks?view=checks")
    cam.shot("21-check-repare", cam.page.get_by_text("Passed").last)


def schedule_tour(cam: Camera) -> None:
    schedule = f"/locations/{MODULE}/schedules/daily_streamify_schedule"
    _command("schedule-list-avant", ["dagster", "schedule", "list", "-m", MODULE])
    cam.goto(schedule)
    switch = cam.page.locator("input[type=checkbox]").first.locator("xpath=..")
    cam.shot("22-schedule-arrete", switch)
    instigator("schedule-start", "schedule", "start", "daily_streamify_schedule")
    _command("schedule-list-apres", ["dagster", "schedule", "list", "-m", MODULE])
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
    instigator("schedule-stop", "schedule", "stop", "daily_streamify_schedule")


def automate_on_upstream_update(cam: Camera) -> None:
    cam.goto("/automation")
    cam.shot("25-automation-liste", cam.text(AUTOMATION_SENSOR))
    instigator("sensor-start-automation", "sensor", "start", AUTOMATION_SENSOR)
    # Laisse au sensor une premiere evaluation: seule une mise a jour posterieure compte.
    time.sleep(45)
    partition = latest_partition()
    assert launch("launch-bronze-seul", "raw_tracks_data", partition) == 0
    runs = wait_until(
        lambda: runs_with_tag("dagster/from_automation_condition", "true"),
        "run lance par l'automatisation",
    )
    status = wait_run_finished(runs[0]["runId"])
    assert status == "SUCCESS", status
    cam.goto("/runs")
    cam.shot(
        "26-run-automatique",
        cam.page.get_by_text("raw_tracks_data", exact=False).first,
        below(
            cam.page.get_by_text(AUTOMATION_SENSOR, exact=False).first.locator(
                "xpath=../.."
            )
        ),
    )
    cam.goto("/assets/clean_tracks?view=automation")
    cam.shot("27-evaluations-automation", cam.text("1 requested"))
    instigator("sensor-stop-automation", "sensor", "stop", AUTOMATION_SENSOR)


def automate_on_file(cam: Camera) -> None:
    instigator("sensor-start-inbox", "sensor", "start", "inbox_file_sensor")
    (DATA / "inbox").mkdir(parents=True, exist_ok=True)
    (DATA / "inbox" / f"{INBOX_PARTITION}.ready").touch()
    runs = wait_until(
        lambda: runs_with_tag("dagster/sensor_name", "inbox_file_sensor"),
        "run lance par le sensor de fichier",
    )
    status = wait_run_finished(runs[0]["runId"])
    assert status == "SUCCESS", status
    sensor = f"/locations/{MODULE}/sensors/inbox_file_sensor"
    cam.goto(sensor)
    cam.shot(
        "28-sensor-fichier",
        cam.page.locator("input[type=checkbox]").first.locator("xpath=.."),
        cam.page.get_by_text("1 run requested", exact=False).first,
    )
    # Le meme fichier ne relance rien: run_key deja traitee.
    (DATA / "inbox" / f"{INBOX_PARTITION}.ready").touch()
    time.sleep(30)
    count = len(runs_with_tag("dagster/sensor_name", "inbox_file_sensor"))
    assert count == 1, count
    instigator("sensor-stop-inbox", "sensor", "stop", "inbox_file_sensor")
    shutil.rmtree(DATA / "inbox")


def automate_on_failure(cam: Camera) -> None:
    instigator("sensor-start-alerte", "sensor", "start", "alert_on_run_failure")
    # A sa premiere evaluation, le sensor se positionne apres les runs existants.
    time.sleep(30)
    assert launch("launch-alerte", ALL_ASSETS, ALERT_PARTITION, BAD_CONFIG) == 1
    alerts = DATA / "alerts"
    found = wait_until(lambda: list(alerts.glob("*.txt")), "fichier d'alerte", 120)
    print(f"[alerte] {found[0].read_text()}", flush=True)
    sensor = f"/locations/{MODULE}/sensors/alert_on_run_failure"
    cam.goto(sensor)
    cam.shot(
        "29-sensor-echec",
        cam.page.locator("input[type=checkbox]").first.locator("xpath=.."),
        cam.text(latest_run_id(ALERT_PARTITION)[:8]),
    )
    instigator("sensor-stop-alerte", "sensor", "stop", "alert_on_run_failure")
    assert launch("launch-alerte-reprise", ALL_ASSETS, ALERT_PARTITION) == 0


def retry_transient_failure(cam: Camera) -> None:
    assert launch("launch-retry", "raw_tracks_data", RETRY_PARTITION, RETRY_CONFIG) == 0
    run_id = latest_run_id(RETRY_PARTITION)
    cam.goto(f"/runs/{run_id}", settle=4000)
    log_filter = cam.page.get_by_placeholder("Filter")
    log_filter.fill("step")
    log_filter.blur()
    cam.page.wait_for_timeout(2500)
    cam.shot(
        "30-run-avec-retry",
        cam.text("STEP_UP_FOR_RETRY"),
        cam.text("STEP_RESTARTED"),
        cam.text("STEP_SUCCESS"),
    )
    code = launch(
        "launch-retry-epuise", "raw_tracks_data", "2024-05-02", EXHAUSTED_CONFIG
    )
    assert code == 1


def partition_dependency(cam: Camera) -> None:
    shutil.copy(WEEKLY_SOURCE, WEEKLY_ACTIVE)
    try:
        cam.reload_definitions()
        assert launch("launch-cumul", "weekly_artist_stats", WEEKLY_PARTITION) == 0
        cam.goto("/assets/weekly_artist_stats?view=events")
        cam.shot(
            "31-cumul-glissant",
            cam.text("window_start"),
            cam.text("window_end"),
        )
    finally:
        WEEKLY_ACTIVE.unlink()
    cam.reload_definitions()


def fast_automation_and_queue(cam: Camera, server: Server) -> None:
    server.stop()
    (HOME / "dagster.yaml").write_text(
        "concurrency:\n  runs:\n    max_concurrent_runs: 2\n"
    )
    server.start(STREAMIFY_FAST_AUTOMATION="1")

    instigator("sensor-start-inbox-file", "sensor", "start", "inbox_file_sensor")
    (DATA / "inbox").mkdir(parents=True, exist_ok=True)
    for partition in QUEUE_PARTITIONS:
        (DATA / "inbox" / f"{partition}.ready").touch()

    def statuses() -> list[str]:
        runs = runs_with_tag("dagster/sensor_name", "inbox_file_sensor")
        return [
            r["status"]
            for r in runs
            if r["status"] in ("QUEUED", "NOT_STARTED", "STARTING", "STARTED")
        ]

    def queue_is_full() -> bool:
        current = statuses()
        return current.count("STARTED") == 2 and current.count("QUEUED") >= 2

    wait_until(queue_is_full, "deux runs en cours, les autres en attente", 120, 1)
    cam.goto("/runs", settle=1500)
    cam.shot(
        "32-file-attente",
        cam.text("Queued"),
        cam.text("Started"),
    )
    wait_until(lambda: not statuses(), "file d'attente videe", 240)
    instigator("sensor-stop-inbox-file", "sensor", "stop", "inbox_file_sensor")
    shutil.rmtree(DATA / "inbox")

    before = len(runs_with_tag("dagster/from_automation_condition", "true", 100))
    instigator("sensor-start-cron", "sensor", "start", AUTOMATION_SENSOR)

    def next_gold_run() -> str | None:
        """Bronze peut partir seul ou avec la suite: on attend le run qui produit Gold."""
        runs = runs_with_tag("dagster/from_automation_condition", "true", 100)
        for run in runs[: len(runs) - before]:
            assets = [a["path"][0] for a in run["assetSelection"] or []]
            if "daily_artist_stats" in assets:
                return run["runId"]
        return None

    run_id = wait_until(next_gold_run, "run du cron jusqu'a Gold", 240)
    status = wait_run_finished(run_id)
    assert status == "SUCCESS", status
    cam.goto("/runs")
    cam.shot(
        "33-run-du-cron",
        below(
            cam.page.get_by_text(AUTOMATION_SENSOR, exact=False).first.locator(
                "xpath=../.."
            )
        ),
    )
    instigator("sensor-stop-cron", "sensor", "stop", AUTOMATION_SENSOR)
    # Un run deja lance peut encore mettre a jour Gold et relancer le chrono.
    time.sleep(30)

    page = "/assets/daily_artist_stats?view=overview"
    wait_until(lambda: freshness() == "HEALTHY", "fraicheur saine", 60)
    cam.goto(page)
    cam.shot("34-fraicheur-saine", below(cam.text("Passing")))
    wait_until(lambda: freshness() == "WARNING", "fraicheur en alerte", 420, 5)
    cam.goto(page)
    cam.shot("35-fraicheur-alerte", below(cam.text("Warning")))
    wait_until(lambda: freshness() == "DEGRADED", "fraicheur en echec", 420, 5)
    cam.goto(page)
    cam.shot("36-fraicheur-echec", below(cam.text("Failed")))
    server.stop()
    (HOME / "dagster.yaml").unlink()
    server.start()


def main() -> None:
    shutil.rmtree(WORK, ignore_errors=True)
    HOME.mkdir(parents=True)
    (WORK / "cli").mkdir()
    shutil.rmtree(IMAGES, ignore_errors=True)
    IMAGES.mkdir(parents=True)

    server = Server()
    server.start()
    try:
        with sync_playwright() as playwright:
            browser = playwright.chromium.launch()
            cam = Camera(new_context(browser).new_page())
            tour_interface(cam)
            materialize_from_ui(cam)
            materialize_from_cli(cam)
            run_backfill(cam)
            break_data_quality(cam)
            schedule_tour(cam)
            automate_on_upstream_update(cam)
            automate_on_file(cam)
            automate_on_failure(cam)
            retry_transient_failure(cam)
            partition_dependency(cam)
            fast_automation_and_queue(cam, server)
            browser.close()
    finally:
        server.stop()
    print(f"Captures dans {IMAGES}")


if __name__ == "__main__":
    main()
