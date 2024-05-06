from subgrounds import Subgrounds
import sys

sg = Subgrounds(timeout=60)

livepeer = sg.load_subgraph(
    "https://gateway.thegraph.com/api/{key}/subgraphs/id/FDD65maya4xVfPnCjSgDRBz6UBWKAcmGtgY6BmUueJCg".format(key=sys.argv[1]))

event_range = livepeer.Query.events(
    orderBy=livepeer.Event.timestamp,
    orderDirection='desc',
    where=[
        livepeer.Event.timestamp > int(sys.argv[2]),
        livepeer.Event.timestamp < int(sys.argv[3]),

    ]
)

df = sg.query_df([
    event_range.id,
    event_range.timestamp
])

df.to_csv("{start_ts}_{end_ts}.csv".format(start_ts=sys.argv[2],end_ts=sys.argv[3]))
