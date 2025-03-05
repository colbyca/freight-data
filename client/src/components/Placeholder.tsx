import { useEffect, useState } from "react";

type PlaceholderProps = {
    prop1: number;
    prop2: string;
}

export const PlaceholderComponent = ({ prop1, prop2 }: PlaceholderProps) => {
    const [state, setState] = useState("");
    useEffect(() => {

    }, []);

    return (
        <div>
            My prop 1 is {prop1} and my prop 2 is {prop2}
        </div>
    );
}