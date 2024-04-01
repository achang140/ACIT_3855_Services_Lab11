import React, { useEffect, useState, useCallback } from 'react';
import '../App.css';

export default function EndpointAudit(props) {
    const [isLoaded, setIsLoaded] = useState(false);
    const [log, setLog] = useState(null);
    const [error, setError] = useState(null);
    const rand_val = Math.floor(Math.random() * 100); // Get a random event from the event store
    const [index, setIndex] = useState(null);

    const getAudit = useCallback(() => {
        fetch(`http://acit3855-lab6-kafka.westus3.cloudapp.azure.com:8110/${props.endpoint}?index=${rand_val}`)
            .then(res => res.json())
            .then(
                (result) => {
                    console.log("Received Audit Results for " + props.endpoint);
                    setLog(result);
                    setIsLoaded(true);
                    setIndex(rand_val);
                },
                (error) => {
                    setError(error);
                    setIsLoaded(true);
                }
            );
    }, [props.endpoint, rand_val]);

    useEffect(() => {
        const interval = setInterval(() => getAudit(), 4000); // Update every 4 seconds
        return () => clearInterval(interval);
    }, [getAudit]);

    if (error) {
        return (<div className={"error"}>Error found when fetching from API</div>);
    } else if (!isLoaded) {
        return (<div>Loading...</div>);
    } else {
        return (
            <div>
                <h3>{props.endpoint}-{index}</h3>
                {JSON.stringify(log)}
            </div>
        );
    }
}

// <h3>{props.endpoint}-{rand_val}</h3>

// import React, { useEffect, useState } from 'react'
// import '../App.css';

// export default function EndpointAudit(props) {
//     const [isLoaded, setIsLoaded] = useState(false);
//     const [log, setLog] = useState(null);
//     const [error, setError] = useState(null)
// 	const rand_val = Math.floor(Math.random() * 100); // Get a random event from the event store

//     const getAudit = () => {
//         fetch(`http://acit3855-lab6-kafka.westus3.cloudapp.azure.com:8110/${props.endpoint}?index=${rand_val}`)
//             .then(res => res.json())
//             .then((result)=>{
// 				console.log("Received Audit Results for " + props.endpoint)
//                 setLog(result);
//                 setIsLoaded(true);
//             },(error) =>{
//                 setError(error)
//                 setIsLoaded(true);
//             })
//     }
// 	useEffect(() => {
// 		const interval = setInterval(() => getAudit(), 4000); // Update every 4 seconds
// 		return() => clearInterval(interval);
//     }, [getAudit]);

//     if (error){
//         return (<div className={"error"}>Error found when fetching from API</div>)
//     } else if (isLoaded === false){
//         return(<div>Loading...</div>)
//     } else if (isLoaded === true){
        
//         return (
//             <div>
//                 <h3>{props.endpoint}-{rand_val}</h3>
//                 {JSON.stringify(log)}
//             </div>
//         )
//     }
// }
