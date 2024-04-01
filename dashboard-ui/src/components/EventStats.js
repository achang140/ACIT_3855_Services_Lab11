import React, { useEffect, useState, useCallback } from 'react';
import '../App.css';

export default function EventStats() {
    const [isLoaded, setIsLoaded] = useState(false);
    const [stats, setStats] = useState({});
    const [error, setError] = useState(null);

    const getStats = useCallback(() => {
        fetch(`http://acit3855-lab6-kafka.westus3.cloudapp.azure.com:8120/event_stats`)
            .then(res => res.json())
            .then(
                (result) => {
                    console.log("Received Evenet Stats");
                    setStats(result);
                    setIsLoaded(true);
                },
                (error) => {
                    setError(error);
                    setIsLoaded(true);
                }
            );
    }, []);

    useEffect(() => {
        const interval = setInterval(() => getStats(), 2000); // Update every 2 seconds
        return () => clearInterval(interval);
    }, [getStats]);

    if (error) {
        return (<div className={"error"}>Error found when fetching from API</div>);
    } else if (!isLoaded) {
        return (<div>Loading...</div>);
    } else {
        return (
            <div>
                <h1>Event Log Stats</h1>
                <table className={"StatsTable"}>
                    <tbody>
                        <tr>
                            <td>0001 Events Logged: {stats['0001']}</td>
                        </tr>
                        <tr>
                            <td>0002 Events Logged: {stats['0002']}</td>
                        </tr>
                        <tr>
                            <td>0003 Events Logged: {stats['0003']}</td>
                        </tr>
                        <tr>
                            <td>0004 Events Logged: {stats['0004']}</td>
                        </tr>
                    </tbody>
                </table>
            </div>
        );
    }
}
