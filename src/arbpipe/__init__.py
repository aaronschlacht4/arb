"""Cross-venue prediction-market arbitrage pipeline.

Stage subpackages, in the order the pipeline runs them:

    resolve     market identifiers   -> events/<slug>/event.json
    fetch       venue APIs           -> events/<slug>/raw/
    clean       raw tapes            -> events/<slug>/clean/
    verify      tapes vs independent ground truth
    arbitrage   clean tapes          -> events/<slug>/results/
    report      results              -> charts
    legacy      dead sources, kept only as provenance

eventlib holds the event config and every output path; portfolio holds
the (y, z) position model. Run the whole thing with arbpipe.cli.
"""
