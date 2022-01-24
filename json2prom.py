import asyncio
import httpx
import json
import logging
import os

from aiohttp import web
from jsonpath_ng import parse

LOG = logging.getLogger(__name__)


class Transformer:
    def __init__(self, nameprefix, labels, sources, interval=300):
        self.nameprefix = nameprefix
        self.labels = labels
        self.sources = sources
        self.interval = interval

        self.metrics = []
        self.lock = asyncio.Lock()

    async def loop(self):
        while True:
            await self.update_metrics()
            await asyncio.sleep(self.interval)

    async def update_metrics(self):
        LOG.info("updating metrics")
        tasks = []
        for src in self.sources:
            for url in src["urls"]:
                tasks.append(asyncio.create_task(self.fetch_url(src, url)))

        res = await asyncio.gather(*tasks)

        async with self.lock:
            self.metrics = []
            for success, src, urlspec, data in res:
                if not success:
                    continue

                for path, name in src["expose"].items():
                    expr = parse(path)
                    match = expr.find(data)
                    if len(match) != 1:
                        continue

                    val = match[0].value

                    if self.nameprefix:
                        metric_name = f"{self.nameprefix}{name}"
                    else:
                        metric_name = name

                    metric = {
                        "name": metric_name,
                        "labels": {
                            "source": src["name"],
                        },
                        "value": val,
                    }
                    if self.labels:
                        metric["labels"].update(self.labels)
                    if "labels" in urlspec:
                        metric["labels"].update(urlspec["labels"])
                    self.metrics.append(metric)

    async def fetch_url(self, src, urlspec):
        url = urlspec["url"].format(**os.environ)
        LOG.info("fetch %s", url)
        async with httpx.AsyncClient() as client:
            try:
                res = await client.get(url)
                res.raise_for_status()
                success = True
            except Exception as err:
                LOG.error("failed to fetch %s: %s", url, err)
                success = False

            return success, src, urlspec, res.json()

    async def get_metrics(self):
        async with self.lock:
            return self.metrics


def start_background_tasks(T):
    async def _start_wrapper(app):
        app["update_metrics"] = asyncio.create_task(T.loop())

    return _start_wrapper


def create_app():
    with open("config.json") as fd:
        config = json.load(fd)

    T = Transformer(config.get("nameprefix"), config.get("labels"), config["sources"])

    routes = web.RouteTableDef()
    app = web.Application()

    app.on_startup.append(start_background_tasks(T))

    @routes.get("/metrics")
    async def metrics(request):
        res = []
        for metric in T.metrics:
            labels = ", ".join(f'{k}="{v}"' for k, v in metric["labels"].items())
            res.append(f"{metric['name']}{{{labels}}} {metric['value']}")

        return web.Response(text="\n".join(res))

    app.add_routes(routes)
    return app


app = create_app()
web.run_app(app)
