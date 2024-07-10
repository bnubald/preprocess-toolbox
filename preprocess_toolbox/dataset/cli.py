def amsr2():
    pass


@app.command()
def cmip6():
    args = process_args(extra_args=[
        (["source"], dict(type=str)),
        (["member"], dict(type=str)),
    ],)
    dates = process_date_args(args)

    cmip = IceNetCMIPPreProcessor(
        args.source,
        args.member,
        args.abs,
        args.anom,
        args.name,
        dates["train"],
        dates["val"],
        dates["test"],
        linear_trends=args.trends,
        linear_trend_days=args.trend_lead,
        north=args.hemisphere == "north",
        parallel_opens=args.parallel_opens,
        ref_procdir=args.ref,
        south=args.hemisphere == "south",
        update_key=args.update_key,
    )
    cmip.init_source_data(lag_days=args.lag,)
    cmip.process()


@app.command()
def era5():
    args = process_args()
    dates = process_date_args(args)

    era5 = IceNetERA5PreProcessor(
        args.abs,
        args.anom,
        args.name,
        dates["train"],
        dates["val"],
        dates["test"],
        linear_trends=args.trends,
        linear_trend_days=args.trend_lead,
        north=args.hemisphere == "north",
        parallel_opens=args.parallel_opens,
        ref_procdir=args.ref,
        south=args.hemisphere == "south",
        update_key=args.update_key,
    )
    era5.init_source_data(lag_days=args.lag,)
    era5.process()


@app.command()
def osisaf():
    args = process_args()
    dates = process_date_args(args)

    osi = IceNetOSIPreProcessor(args.abs,
                                args.anom,
                                args.name,
                                dates["train"],
                                dates["val"],
                                dates["test"],
                                linear_trends=args.trends,
                                linear_trend_steps=args.trend_lead,
                                north=args.hemisphere == "north",
                                parallel_opens=args.parallel_opens,
                                ref_procdir=args.ref,
                                south=args.hemisphere == "south")
    osi.init_source_data(lag_days=args.lag, )
    osi.process()
