import { MapContainer, TileLayer, useMap } from "react-leaflet"
import { StopMarkerProps, StopMarkers } from "../components/StopMarkers"
import { JSX } from "react/jsx-runtime";
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import { useState } from "react";
import { RouteProps, Routes } from "../components/Routes";
import MarkerModel from "../Models/MarkerModel";

interface MapInfo {
    stopMarkerProps: StopMarkerProps;
    routeProps: RouteProps;
}

export const VisualMap = () => {
    const [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.STOPS);
    const [sidebarHidden, setSidebarHidden] = useState<boolean>(false);
    // We are likely going to use a useEffect hook to query the api for relevant map information
    const [mapInfo, setMapInfo] = useState<MapInfo>();

    let markers: JSX.Element[] = [];
    
    switch (selectedMode.valueOf()) {
        case MapViewType.STOPS:
            markers = StopMarkers({markers: [new MarkerModel(40.74404335285939, -111.89270459860522, "red")]});
            break;
        case MapViewType.ROUTES:
            markers = Routes();
            break;
    }
    
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