import schemas


class BettingWrapper():
    requires_browser = False
    def __init__(self):
        self.bookmaker = "undefined_bookmaker"
        self.requires_browser = False
    async def run(self, sportToSelect : str | None = None, linkIndex : int = 0, field: str | None = None):
        raise NotImplementedError("Subclasses should implement this!")
    async def rescanEvent(self, link : str):
        raise NotImplementedError("Subclasses should implement this!")
    
    async def scrapeGame(self, link: str, event : schemas.Event,\
                        oghome : str, ogaway : str,  is_event_url : bool, index : int | None = None, disable_scroll : bool = True):
        raise NotImplementedError("Subclasses should implement this!")

