"""Project modules (plugins) for domain-specific logic.

Survey-specific implementations, try to get this like a module system

Handles WALLABY-specific workflow generation and processing.
# - WALLABY dataset validation
# - DALiuGE workflow manifest generation
# - ASKAPsoft pipeline configuration
# - Result processing and validation
# https://github.com/ICRAR/wallaby-hires/blob/main/

so thinking perhaps an entry point?
    [project.entry-points."beampipe.projects"]
    wallaby_hires = "wallaby_hires.module"
"""