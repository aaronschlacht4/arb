"""Cross-venue prediction-market arbitrage pipeline.

Stage subpackages, in the order the pipeline runs them:

    resolve     market identifiers   -> events/<slug>/event.json
    fetch       venue APIs           -> events/<slug>/1_raw/{kalshi,polymarket}/
    clean       raw tapes            -> events/<slug>/2_clean/{kalshi,polymarket}/
    verify      tapes vs independent ground truth
    arbitrage   clean tapes          -> events/<slug>/3_sorted/, 4_arbitrage/
    report      arbitrage            -> events/<slug>/5_charts/
    legacy      dead sources, kept only as provenance

eventlib holds the event config and every output path; portfolio holds
the (y, z) position model. Run the whole thing with arbpipe.cli.
"""
