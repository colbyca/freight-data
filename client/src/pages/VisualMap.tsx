import { MapContainer, TileLayer } from "react-leaflet"
import { StopMarkerProps, StopMarkers } from "../components/StopMarkers"
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import React, { ReactNode, useEffect, useRef, useState } from "react";
import { RouteProps, Routes, Route } from "../components/Routes";
import MarkerModel from "../Models/MarkerModel";
import HeatmapLayer, { HeatLayerProps } from "../components/HeatmapLayer";
import { MapEventHandler } from "../components/MapEventHandler";
import { LatLng, LatLngBounds, LatLngTuple } from "leaflet";
import { LoadingElement } from "../components/LoadingElement";

interface MapInfo {
    stopMarkerProps?: StopMarkerProps;
    routeProps?: RouteProps;
    heatmapProps?: HeatLayerProps;
}

export type RouteInfo = {
    latitude: number;
    longitude: number;
    truck_id: string;
    route_id: string;
}

export const VisualMap = () => {
    const [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.NONE);
    const [sidebarHidden, setSidebarHidden] = useState<boolean>(false);
    // We are likely going to use a useEffect hook to query the api for relevant map information
    const mapInfo = useRef<MapInfo>({ heatmapProps: { latlngs: [] } });
    const [markers, setMarkers] = useState<ReactNode>([]);
    const [bounds, setBounds] = useState<LatLngBounds>(new LatLngBounds(new LatLng(-90, 180), new LatLng(90, -180)));
    const [loading, setLoading] = useState<boolean>(false);

    useEffect(() => {
        updateSelectedMode(selectedMode, mapInfo, bounds, setLoading, setMarkers);
    }, [selectedMode]);

    // useEffect(() => {
    //     updateBounds(selectedMode, mapInfo, bounds, setMapInfo, setMarkers)
    // }, [bounds]);


    return (
        <div id="super-container">
            <div className="layout-container">
                {loading ? <LoadingElement /> : (
                    <MapContainer center={[41, -112]} zoom={6} scrollWheelZoom={false}>
                        <TileLayer
                            attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                            url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                        {markers}
                        <MapEventHandler setBounds={setBounds} />
                    </MapContainer>
                )}
            </div>
            <div className="sidebar-container" data-hidden={sidebarHidden}>
                <button type="button"
                    id="hide-btn"
                    onClick={() => setSidebarHidden(!sidebarHidden)}>
                    {sidebarHidden ? "<" : ">"}
                </button>
                {!sidebarHidden && <MapSidebar selectedMode={selectedMode} setSelectedMode={setSelectedMode} />}
            </div>
        </div>
    )
}

const updateSelectedMode = async (
    selectedMode: MapViewType,
    mapInfo: React.MutableRefObject<MapInfo>,
    bounds: LatLngBounds,
    setLoading: React.Dispatch<boolean>,
    setMarkers: React.Dispatch<React.SetStateAction<ReactNode>>
) => {
    setLoading(true);
    let result;
    switch (selectedMode.valueOf()) {
        case MapViewType.STOPS:
            mapInfo.current.stopMarkerProps = { markers: [new MarkerModel(40.74404335285939, -111.89270459860522, "red")] };
            setMarkers(StopMarkers(mapInfo.current.stopMarkerProps))
            break;
        case MapViewType.ROUTES:
            //mapInfo.current.routeProps = {routes: [new Route([[41.742575, -111.81137], [40.7605868, -111.8335]])]};
            //setMarkers(Routes(mapInfo.current.routeProps));
            result = (await toUtahCall(new Date(2023, 0, 1), new Date(2023, 1, 0))).result;
            mapInfo.current.routeProps = { routes: result }
            setMarkers(Routes(mapInfo.current.routeProps))
            break;
        case MapViewType.HEAT:
            result = (await heatmapCall(new Date(2023, 0, 1), new Date(2023, 0, 31))).result;
            mapInfo.current.heatmapProps = { latlngs: result.heatmap_data };
            setMarkers(<HeatmapLayer max={4} latlngs={mapInfo.current.heatmapProps.latlngs} />);
            break;
        case MapViewType.NONE:
            break;
    }
    setLoading(false);
}

const updateBounds = async (
    selectedMode: MapViewType,
    mapInfo: MapInfo,
    bounds: LatLngBounds,
    setMapInfo: React.Dispatch<React.SetStateAction<MapInfo>>,
    setMarkers: React.Dispatch<React.SetStateAction<ReactNode>>
) => {
    switch (selectedMode.valueOf()) {
        case MapViewType.HEAT: {

        }
    }
}

const heatmapCall = async (startDate: Date, endDate: Date) => {
    let heatmapResponse = await fetch(`/api/queries/heatmap`, {
        method: "POST",
        headers: [["Content-Type", "application/json"],],
        body: JSON.stringify({
            month: startDate.getMonth() + 1,
            eps: 0.001,
            minSamples: 3,
            startDate: startDate.toISOString(),
            endDate: endDate.toISOString(),
        })
    });

    let jobId: number = (await heatmapResponse.json())["jobId"];
    let result = await waitUntilResult(jobId);

    result["result"]["heatmap_data"] = result["result"]["heatmap_data"].map((item: any) => {
        return [item.latitude, item.longitude, item.count]
    });

    return result;
}

// router.post('/to_utah', async (req: Request, res: Response) => {
//     try {
//         // Validate request body
//         const { month, startDate, endDate } = utahBoundarySchema.parse(req.body);

const toUtahCall = async (startDate: Date, endDate: Date) => {
    let response = await fetch(`/api/queries/to_utah`, {
        method: "POST",
        headers: [["Content-Type", "application/json"],],
        body: JSON.stringify({
            month: startDate.getMonth() + 1,
            startDate: startDate.toISOString(),
            endDate: endDate.toISOString(),
        })
    });

    let jobId: number = (await response.json())["jobId"];
    let result = await waitUntilResult(jobId);
    result.result = result.result.reduce(
        (entryMap: any, e: any) => entryMap.set(e.route_id, [...entryMap.get(e.route_id) || [],
        { latitude: e.latitude, longitude: e.longitude, truck_id: e.truck_id, route_id: e.route_id }
        ]),
        new Map()
    ).values().map((value: RouteInfo[]) => new Route(value));


    return result;
}

const waitUntilResult = async (jobId: number) => {
    let jobComplete = false;
    let result;
    while (!jobComplete) {
        let jobStatusResponse = await fetch(`/api/queries/status/${jobId}`, {
            headers: [['cache-control', 'no-cache']]
        });

        result = await jobStatusResponse.json();

        if (result["completedAt"] != null) {
            return result
        }

        if (result["error"] != null) {
            console.error(`Error for job ${jobId}: ${result["error"]}`);
            return;
        }

        await new Promise(resolve => setTimeout(resolve, 3000));
    }
}