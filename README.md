# TorrentStreamingServer

"""
The Hub - Torrent Streaming Server
-----------------------------------
Streams the largest media file inside a torrent straight into an HTML5
<video> element via an on-the-fly FFmpeg remux. There is intentionally no
"download" affordance anywhere in the UI or the API - only the STREAM path
is exposed to the browser.

Notable improvements over the original version:
  - DHT / LSD / UPnP / NAT-PMP enabled on the libtorrent session (with
    bootstrap DHT routers) so magnets actually resolve peers reliably,
    instead of relying on trackers alone.
  - Torrent metadata/TMDB lookups run concurrently (thread pool) instead of
    sequentially, so search results come back much faster.
  - Real error handling: search failures, unreachable APIs and torrent
    metadata timeouts all surface a readable message in the UI instead of
    hanging forever or crashing.
  - Idle torrent sessions are garbage collected automatically after an hour
    of inactivity so long-running instances don't leak handles/disk.
  - The stream endpoint validates its inputs (info-hash shape, magnet
    prefix) and bounds every wait loop with a timeout.
  - threaded=True on the dev server + debug reloader disabled, so one
    client streaming video no longer blocks every other request (and the
    libtorrent session no longer gets started twice by the reloader).
  - The "download"/magnet button has been removed entirely - the grid only
    offers STREAM, and no raw magnet link is rendered into the page.
"""
