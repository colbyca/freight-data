import { MapContainer, TileLayer, useMap } from "react-leaflet"
import { StopMarkers } from "../components/StopMarkers"
import { JSX } from "react/jsx-runtime";
import 'leaflet/dist/leaflet.css';
import { MapSidebar, MapViewType } from "../components/MapSidebar";
import { useState } from "react";
import { Polylines } from "../components/PolyLines";
import MarkerModel from "../Models/MarkerModel";

export const VisualMap = () => {
    const [selectedMode, setSelectedMode] = useState<MapViewType>(MapViewType.STOPS);

    let markers: JSX.Element[] = [];
    
    switch (selectedMode.valueOf()) {
        case MapViewType.STOPS:
            markers = StopMarkers({markers: [new MarkerModel(40.74404335285939, -111.89270459860522, "red")]});
            break;
        case MapViewType.ROUTES:
            markers = Polylines();
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
        <MapSidebar selectedMode={selectedMode} setSelectedMode={setSelectedMode}/>
        </div>
    )
}