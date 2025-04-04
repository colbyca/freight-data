import { MapContainer, TileLayer } from "react-leaflet"
import { StopMarkerProps, StopMarkers } from "../components/StopMarkers"
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import { ReactNode, useEffect, useState } from "react";
import { RouteProps, Routes, Route } from "../components/Routes";
import MarkerModel from "../Models/MarkerModel";
import HeatmapLayer, { HeatLayerProps } from "../components/HeatmapLayer";
import { MapEventHandler } from "../components/MapEventHandler";
import { LatLng, LatLngBounds } from "leaflet";
import { LoadingElement } from "../components/LoadingElement";

interface MapInfo {
    stopMarkerProps?: StopMarkerProps;
    routeProps?: RouteProps;
    heatmapProps?: HeatLayerProps;
}

export const VisualMap = () => {
    const [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.NONE);
    const [sidebarHidden, setSidebarHidden] = useState<boolean>(false);
    // We are likely going to use a useEffect hook to query the api for relevant map information
    const [mapInfo, setMapInfo] = useState<MapInfo>({heatmapProps: {latlngs: []}});
    const [markers, setMarkers] = useState<ReactNode>([]);
    const [bounds, setBounds] = useState<LatLngBounds>(new LatLngBounds(new LatLng(-90, 180), new LatLng(90, -180))); 
    const [loading, setLoading] = useState<boolean>(true);

    useEffect(() => {
        updateSelectedMode(selectedMode, mapInfo, bounds, setMapInfo, setMarkers);
    },[selectedMode]);

    // useEffect(() => {
    //     updateBounds(selectedMode, mapInfo, bounds, setMapInfo, setMarkers)
    // }, [bounds]);

    
    return (
        <div id="super-container">
        <div className="layout-container">
            {loading ?  <LoadingElement/> : (
                <MapContainer center={[41, -112]} zoom={6} scrollWheelZoom={false}>
                    <TileLayer
                        attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                        url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
                    {markers}
                    <MapEventHandler setBounds={setBounds}/>
                </MapContainer>
            )}
        </div>
        <div className="sidebar-container" data-hidden={sidebarHidden}>
            <button type="button"
                    id="hide-btn"
                    onClick={() => setSidebarHidden(!sidebarHidden)}>
                {sidebarHidden ? "<" : ">"}
            </button>
            {!sidebarHidden && <MapSidebar selectedMode={selectedMode} setSelectedMode={setSelectedMode}/>}
        </div>
        </div>
    )
}

const updateSelectedMode = async (
    selectedMode: MapViewType,
    mapInfo: MapInfo,
    bounds: LatLngBounds,
    setMapInfo: React.Dispatch<React.SetStateAction<MapInfo>>,
    setMarkers: React.Dispatch<React.SetStateAction<ReactNode>>
) => {

        switch (selectedMode.valueOf()) {
            case MapViewType.STOPS:
                mapInfo.stopMarkerProps = {markers: [new MarkerModel(40.74404335285939, -111.89270459860522, "red")]};
                setMarkers(StopMarkers(mapInfo.stopMarkerProps))
                break;
            case MapViewType.ROUTES:
                mapInfo.routeProps = {routes: [new Route([[41.742575, -111.81137], [40.7605868, -111.8335]])]};
                setMarkers(Routes(mapInfo.routeProps));
                break;
            case MapViewType.HEAT:
                // mapInfo.heatmapProps = {latlngs: [[41.742575, -111.81137,1], [40.7605868, -111.8335,1]]};
                // setMapInfo(mapInfo);
                // setMarkers(<HeatmapLayer minOpacity={0.5} latlngs={mapInfo.heatmapProps!.latlngs}/>);
                await heatmapCall(new Date(2023, 0,1), new Date(2023,0,13));
                break;
            case MapViewType.NONE:
                break;
        }
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
        headers: [["Content-Type", "application/json"], ],
        body: JSON.stringify({
            month: startDate.getMonth() + 1,
            eps: 0.001,
            minSamples: 3,
            startDate: startDate.toISOString(),
            endDate: endDate.toISOString(),
        })
    });

    let jobId: number = (await heatmapResponse.json())["jobId"];
    let jobComplete = false;
    let result;
    while (!jobComplete) {
        let jobStatusResponse = await fetch(`/api/queries/status/${jobId}`, {
            headers: [['cache-control', 'no-cache']]
        });

        result = await jobStatusResponse.json();

        if (result["completedAt"] != null) {
            break;
        }

        await new Promise(resolve => setTimeout(resolve,3000));
    }
    console.log(result)
}
