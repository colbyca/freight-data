import { LatLngTuple } from "leaflet";
import { Polyline } from "react-leaflet"
import { RouteInfo } from "../pages/VisualMap";


export interface RouteProps {
    routes: Route[]
}

export class Route {
    positions: LatLngTuple[]
    route_id: string
    truck_id: string

    constructor(routeInfo: RouteInfo[]) {
        this.positions = routeInfo.map((info) => [info.latitude, info.longitude]);
        this.route_id = routeInfo[0].route_id;
        this.truck_id = routeInfo[0].truck_id;
    }
}

export const Routes = (props: RouteProps) => {
    return props.routes.map((route, index) => {
        console.log(route);
        return (
            <Polyline key={index} positions={route.positions} />
        )
    });
}