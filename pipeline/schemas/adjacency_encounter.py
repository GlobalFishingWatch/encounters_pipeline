
adjacency_encounter = {
    "fields": [
        {
            "name": "timestamp",
            "mode": "NULLABLE",
            "type": "TIMESTAMP",
            "description": "The timestamp of the adjacency position."
        },
        {
            "name": "seg_id",
            "mode": "NULLABLE",
            "type": "STRING",
            "description": "The segment id of the adjacency position."
        },
        {
            "name": "lat",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "The latitude of the adjacency position."
        },
        {
            "name": "lon",
            "mode": "NULLABLE",
            "type": "FLOAT",
            "description": "The longitude of the adjacency position."
        },
        {
            "name": "adjacent_segments",
            "mode": "REPEATED",
            "type": "RECORD",
            "description": "Information about segments adjacent to the vessel.",
            "fields": [
                {
                    "name": "seg_id",
                    "mode": "NULLABLE",
                    "type": "STRING",
                    "description": "The segment id of the adjacent segment."
                },
                {
                    "name": "lat",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The latitude of the adjacent segment."
                },
                {
                    "name": "lon",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The longitude of the adjacent segment."
                },
                {
                    "name": "distance",
                    "mode": "NULLABLE",
                    "type": "FLOAT",
                    "description": "The distance to the adjacent segment in meters."
                }
            ],
        },
    ]
}
