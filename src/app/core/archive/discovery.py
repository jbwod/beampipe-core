"""Archive discovery service.

Handles polling and event-driven discovery of newly deposited datasets.
"""

# - Polling-based discovery for CASDA
# - Event-driven discovery (webhooks, message queues or something idk)
# - Archive adapter pattern for different archives

# using the ARQ queue system in /workers and /tasks
# going to use this for discovery and polling tasks