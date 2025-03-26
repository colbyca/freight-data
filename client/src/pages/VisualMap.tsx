import { MapContainer, TileLayer, useMap } from "react-leaflet"
import { StopMarkerProps, StopMarkers } from "../components/StopMarkers"
import { JSX } from "react/jsx-runtime";
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import { ReactNode, useEffect, useState } from "react";
import { RouteProps, Routes, Route } from "../components/Routes";
import MarkerModel from "../Models/MarkerModel";

interface MapInfo {
    stopMarkerProps: StopMarkerProps;
    routeProps: RouteProps;
}

export const VisualMap = () => {
    const [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.NONE);
    const [sidebarHidden, setSidebarHidden] = useState<boolean>(false);
    // We are likely going to use a useEffect hook to query the api for relevant map information
    const [mapInfo, setMapInfo] = useState<MapInfo>({stopMarkerProps: {markers: []}, routeProps: {}});
    const [markers, setMarkers] = useState<ReactNode>([]);

    useEffect(() => {
        switch (selectedMode.valueOf()) {
            case MapViewType.STOPS:
                mapInfo.stopMarkerProps = {markers: [new MarkerModel(40.74404335285939, -111.89270459860522, "red")]};
                setMarkers(StopMarkers(mapInfo.stopMarkerProps));
                break;
            case MapViewType.ROUTES:
                mapInfo.routeProps = {routes: [new Route([[41.742575, -111.81137], [40.7605868, -111.8335]])]}
                setMarkers(Routes(mapInfo.routeProps))
                break;
            case MapViewType.NONE:
                break;
        }
        setMapInfo(mapInfo);
    },[selectedMode]);


    
    return (
        <div id="super-container">
        <div className="layout-container">
        <MapContainer center={[41, -112]} zoom={6} scrollWheelZoom={false}>
            <TileLayer
                attribution='&copy; <a href="https://www.openstreetmap.org/copyright">OpenStreetMap</a> contributors'
                url="https://{s}.tile.openstreetmap.org/{z}/{x}/{y}.png" />
            {markers}
        </MapContainer>
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