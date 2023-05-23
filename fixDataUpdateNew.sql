CREATE PROCEDURE [dbo].[fixDataUpdateNew] 
--универсальная процедура, используемая для:
 -- 1) обработки данных (список инструментов, маркет-дата и стаканы), полученных через FIX-протокол и в выходном параметре выдается итоговое состояние инструмента или стакана
 -- 2) выдачи данных по запросу (например, котировки для клиента)
	@messageText   nvarchar(MAX),		--текст запроса в формате json
	@resCode	   varchar(10) output,	--код возврата
	@resultString  nvarchar(MAX) output,	--результат в формате json
	@messageType   varchar(20) = 'Normal'
AS
BEGIN

DECLARE @objectId		bigint
DECLARE @propertyID		int
DECLARE @objectTypeOpv	int
DECLARE @cntRow			int
DECLARE @typeRecOut		tinyint	
DECLARE @brokerCode		varchar(20)
DECLARE @isReal			tinyint
		
insert into dbo.xmlLog(functionName, xmlIn)
		values ('fixDataUpdateNew' + case when isnull(@messageType,'') = '' then '' else ', @messageType=' + @messageType end, @messageText)
	
DECLARE @typeQuery		varchar(50)
DECLARE @objectType		varchar(50)
DECLARE @lang			varchar(50)
DECLARE @sourceName		varchar(50)
DECLARE @stringTmp		nvarchar(MAX)
DECLARE @urlTelegram	varchar(MAX)

declare @acc as varchar(50) 
declare @aggrAccNumMubasher as varchar(50)
declare @bloom_exchCode as varchar(50) 
declare @board varchar(50) 
declare @clientid as varchar(50) 
declare @clientName as nvarchar(200)  
declare @comments as nvarchar(1000) 
declare @commissionContragent as decimal(18,6) 
declare @country as varchar(50) 
declare @currency as varchar(50) 
declare @direction as varchar(50) 
declare @exchangeCode as varchar(50)  
declare @executionTime as varchar(50) 
declare @expirationDate as varchar(50) 
declare @inputDateAIS as varchar(50) 
declare @investor as varchar(50) 
declare @isin as varchar(50) 
declare @isMMorder as varchar(50) 
declare @leavesQty as varchar(50) 
declare @newOrder as int 
declare @orderDateAIS as varchar(50) 
declare @orderID as varchar(50) 
declare @orderID_AIS as varchar(50) 
declare @orderNumberAIS as varchar(50)  
declare @orderReferenceExchange as varchar(50)
declare @dealNumber as varchar(50)
declare @orderStatus as varchar(50)  
declare @origClOrderID as varchar(50)  
declare @price as varchar(50) 
declare @priceDeal as varchar(50) 
declare @priceAvg as varchar(50) 
declare @quantity as varchar(50)
declare @quantityDeal as varchar(50)  
declare @quantityTotal as varchar(50)    
declare @rejectingOrderID as varchar(50) 
declare @serial as varchar(50)
declare @settlementDate as date
declare @settlementDateStr as varchar(50)
declare @ticker as varchar(50) 
declare @tickerNew as varchar(50) 
declare @timeInForce as varchar(50) 
declare @typeLimit as varchar(50) 
declare @userName as varchar(50) 
declare @tradingSessionSubID as varchar(50) 
declare @tradSesStatus as varchar(50) 
declare @tradSesStartTime as varchar(50) 
declare @tradText as varchar(500) 
declare @msgType as varchar(50) 
declare @msgNum as varchar(50) 
declare @sendingTime as varchar(50) 

declare @startTime		as datetime	--время начала подачи заявок на биржу (указываем в UTC)
declare @endTime		as datetime	--время окончания подачи заявок на биржу (указываем в UTC)
declare @diffMinutes	as int		--разница в минутах между нашим временем и UTC (отрицательное значение уже задано в БД)


--по умолчанию ставим в ошибочное состояние
		
set @resCode = '-1'

set @brokerCode = dbo.getSettingsTP('brokerCode');
set @diffMinutes = cast(dbo.getSettingsTP('UTC_different_minutes') as int); 
set @urlTelegram = dbo.getSettingsTP('telegramBot_Broker');


begin try
	--парсим json
	
	SELECT @typeQuery = value FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'typeQuery';
	SELECT @objectType = value FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'objectType';
	SELECT @sourceName = replace(value,'null','0') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'sourceName';	
	SELECT @isReal = value FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'isReal';
	
	if @isReal is null
		set @isReal = dbo.getSettingsTP('isRealBase'); 
			
end try		
begin catch
	insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, PARSE JSON, @isReal=' + cast(@isReal as char(1)), ERROR_MESSAGE())
	set @resCode = '-1'
	return
end catch			


if upper(@objectType) = 'FIX_CFG' and upper(@typeQuery) = 'SELECT'
	--если это запрос конфигурации для подключения FIX
	begin
		SELECT @isMMorder = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'isMMOrder'; --вероятно тут прилетает true или false!!!
		SELECT @exchangeCode = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'ExchCode';
		
		--select @resultString = dbo.getSettingsTP(case 
		--											when upper(@exchangeCode) = 'MUBASHER' and @isReal = 0 then 'fixMubasherTest'
		--											when upper(@exchangeCode) = 'MUBASHER' and @isReal = 1 then 'fixMubasherReal'
		--											when upper(@exchangeCode) = 'AIX' and @isReal = 0  then case when @isMMorder = 'true' then 'fixAIXMMTest' else 'fixAIXTest' end
		--											when upper(@exchangeCode) = 'AIX' and @isReal = 1 then 'fixAIXReal'
													
		--											when upper(@exchangeCode) = 'KASE' and @isReal = 0 then 'fixKASETest'
		--											when upper(@exchangeCode) = 'KASE' and @isReal = 1 then 'fixKASEReal'
		--											else ''
		--										 end)
		
		set @resultString = dbo.getSettingsTP('fix' + isnull(@exchangeCode,'') + case when @isMMorder = 1 then 'MM' else '' end + case when @isReal = 1 then 'Real' else 'Test' end)

		if isnull(@resultString,'') = ''
			set @resCode = '-1'
		else 
			set @resCode = '0'
		
		return
	end

else if upper(@objectType) = 'WORKING_STATUS' and upper(@typeQuery) = 'SELECT'
	--если это запрос состояния запуска/остановки FIX
	begin
		SELECT @exchangeCode = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'exchangeCode';
		
		set @resCode = '0'
		set @resultString = 'STOP'
		
		--проверяем, если есть явно выставленное значение STOP
		if exists (select ID  
					  from dbo.workingStatus 
					   where exchangeCode = @exchangeCode
						 and isReal = @isReal
						 and isStop = 1)

			begin
				set @resCode = '0'
				set @resultString = 'STOP'
				return
			end
		
		--если это код Маркет-мейкера, то убираем лишние символы в коде бирже
		if right(@exchangeCode,2) = 'MM'
			set @exchangeCode = SUBSTRING(@exchangeCode,1,len(@exchangeCode)-2)

		--проверка на рабочий день
		if dbo.ThisIsWorkDayCountry(dateadd(MINUTE,@diffMinutes,getdate()),@exchangeCode) = 0  --upd. getdate() 25.07.2021
			begin
				set @resCode = '0'
				set @resultString = 'STOP'
				return
			end
		else
			begin
				--для проверки на время работы бирж используем время UTC					
				set @startTime = convert(datetime, convert(varchar, dateadd(MINUTE,@diffMinutes,getdate()), 23) + ' ' + dbo.getSettingsTP(@exchangeCode + '_FIX_start_time'), 120)
				set @endTime = convert(datetime, convert(varchar, dateadd(MINUTE,@diffMinutes,getdate()), 23) + ' ' + dbo.getSettingsTP(@exchangeCode + '_FIX_end_time'), 120)					

				--проверяем на попадание текущего времени в диапазон работы биржи 
				if dateadd(MINUTE,@diffMinutes,getdate()) not between @startTime and @endTime
					begin
						set @resCode = '0'
						set @resultString = 'STOP'
						return
					end						
			end		

		set @resCode = '0'
		set @resultString = 'OK'

		return
	end

else if upper(@objectType) = 'AIS_ORDER' and upper(@typeQuery) = 'UPDATE'
--если это заявка из АИС для отправки на FIX
	begin
		
		SELECT @orderID_AIS = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'order_id';	
		SELECT @clientid = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'client_id';
		SELECT @clientName = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'client_fio';
		SELECT @acc = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'cd_account';
		SELECT @investor = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'investor';
		SELECT @orderNumberAIS = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'order_number';
		SELECT @orderDateAIS = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'order_date';
		SELECT @inputDateAIS = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'registered_date';
		SELECT @rejectingOrderID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'rejecting_order_id';
		SELECT @isin = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'isin';
		SELECT @ticker = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'ticker';
		SELECT @price = replace(value,'null','0') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'price';
		SELECT @quantity = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'quantity';
		SELECT @currency = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'currency';
		SELECT @expirationDate = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'duration_date';
		SELECT @exchangeCode = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'bond_market_id';
		SELECT @direction = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'order_type_id';
		SELECT @typeLimit = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'order_kind_id';
		SELECT @userName = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'userName';
		
		if @exchangeCode in ('AIX','AIXMM') and @brokerCode = 'BCC'
		--если это AIX и маркет-мейкер БЦК, то генерируем @orderID_AIS
			begin
				SELECT @isMMorder = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'isMMOrder'; 
				if @isMMorder = '1' and isnull(@orderID_AIS,'') = ''
					set @orderID_AIS = NEWID()
				if  @isMMorder = '1' and (isnull(@acc,'') = '' or  isnull(@acc,'undefined') = 'undefined') 
					begin
						set @acc = case when @isReal = 1 then 'T06ML' else 'T12ML' end
						set @investor = 'MM'
					end
			end
		

		select @exchangeCode = case @brokerCode
									when 'Jysan' then case @exchangeCode 
														when '321' then 'Mubasher' 
														when '313' then 'AIX' 
														else @exchangeCode 
													end
									else @exchangeCode
								end



		if @exchangeCode in ('AIX') and isnull(@investor,'') = '' --add 17.11.2021
			begin
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), 'не указан investor; @orderID_AIS=' + @orderID_AIS)
				set @resCode = '-1'
				set @resultString = 'не указан investor'
				return
			end

		if @exchangeCode in ('Mubasher') and @currency not in ('USD','GBP') --исключили 'EUR' вообще 11.03.2021
			begin
				--insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), @currency + ' - не обрабатываем')
				set @resCode = '-1'
				set @resultString = @currency + ' - не обрабатываем'
				return
			end

		if @direction in ('1', 'BUY') and @typeLimit in ('2','MARKET') and @quantity in ('0','0.00')
			begin
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), 'рыночная покупка с 0 - не обрабатываем')
				set @resCode = '-1'
				set @resultString = 'рыночная покупка с 0 - не обрабатываем'
				return
			end

		if isnull(@price,'') = ''
			set @price = '0'
		


		if not exists (select Id from [newOrders] 
						where [orderID_AIS] = @orderID_AIS 
						  and [isReal] = @isReal
						  --and datediff(DAY,convert(date, @orderDateAIS, 23),cast(getDate() As Date)) between 0 and 1
						  )
			--проверяем, нет ли уже такой заявки в таблице newOrders, а также что это не старый приказ (вчерашний или сегодняшний)
			begin try
				
				if @exchangeCode in ('Mubasher')
					begin
						--проверка на наличие кода для Мубашера у данного инструмента
						begin try
							select @tickerNew = case 
													 when CHARINDEX('...', codeMubasher) > 0 then SUBSTRING(codeMubasher, 1, CHARINDEX('...', codeMubasher)) --если код в Мубашере заканчивается на точку
													 else SUBSTRING(codeMubasher, 1, CHARINDEX('..', codeMubasher)-1)
												end,	--парсим тикер вида BMY..NYSE
								   @bloom_exchCode = case 
														 when CHARINDEX('...', codeMubasher) > 0 then SUBSTRING(codeMubasher, CHARINDEX('..', codeMubasher)+3, 20) --если код в Мубашере заканчивается на точку
														 else SUBSTRING(codeMubasher, CHARINDEX('..', codeMubasher)+2, 20)
													end
							 from [drivers_beSQL_new].[dbo].[instrsNew]
							  where nin = @isin
								and isBlocked = 0
								and exchangeCode = 'INTL'
								and isnull(codeMubasher,'-') <> '-'

							if @bloom_exchCode = 'NYSE MKT'		--делаем замену обозначения 'NYSE MKT' на 'AMEX' из-за разных кодов в системах Мубашера
								set @bloom_exchCode = 'AMEX'

							if @currency = 'EUR' and @bloom_exchCode = 'CHIX'
								--добавили проверку на CHIX в EUR 10.03.2021, позже вообще отключили EUR выше
								begin
									--insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), @bloom_exchCode + ' ' + @currency + ' - не обрабатываем')
									set @resCode = '-1'
									set @resultString = @currency + ' - не обрабатываем'
									return
								end
						end try
						begin catch
							set @resCode = '-1'
							return
						end catch

						if @bloom_exchCode is null or @tickerNew is null
							begin try 
								insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew', 'select from instrsNew @isin=' + @isin + ' не смогли определить инструмент в instrsNew')
								set @resCode = '-1'
								set @resultString = 'Не смогли определить инструмент в instrsNew'
								return
							end try
							begin catch
								set @resCode = '-1'
								return
							end catch

					end
				else
					set @bloom_exchCode = @exchangeCode
				
				if @exchangeCode in ('Mubasher')
					begin
						
						--если это приказ на отмену (вне зависимости от часов работы биржи), и если отменяемого КЗ еще нет в таблице newOrders (значит в Мубашер мы его не отправляли),
						--то просто исполняем этот приказ на отмену (вызывая функцию в АИСе) 18.04.2021
						if @direction in ('3','CANCEL_BUY','4','CANCEL_SELL') 
							begin try
								if not exists (select Id from [newOrders] 
												where [orderID_AIS] = @rejectingOrderID 
												  and [isReal] = @isReal
											   )
									begin try
										declare @urlAddr		nvarchar(MAX)
										declare @return_value	int
										DECLARE @respCode		varchar(10)
										DECLARE @respMessage	varchar(max)

										--проставляем отметку об исполнении приказа на отмену
										set @urlAddr = dbo.getSettingsTP( case @isReal when 1 then 'addrTradeServiceReal' when 0 then 'addrTradeServiceTest' else '' end) 
													+ 'callCompleteRejectOrder?' 
													+ 'clientId=' + @clientID 
													+ '&orderID_AIS=' + @orderID_AIS 
													+ '&user_id=1' --1 - это видимо юзер-админ в АИС

										EXEC @return_value = dbo.httpGetResponse 
												@url = @urlAddr
											   ,@respCode = @respCode OUTPUT
											   ,@respMessage = @respMessage OUTPUT --ответ от сервиса
				
										insert into dbo.logEvents(objectName, messageText)
											values ('fixDataUpdateNew, callCompleteRejectOrder', '@urlAddr=' + @urlAddr +', @respMessage=' + @respMessage)
					
										if CHARINDEX('"code":"0"',@respMessage) = 0
											begin try
												insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, callCompleteRejectOrder', '@orderID_AIS=' + @orderID_AIS + '; ' + @respMessage)

												set @messageText = @urlTelegram + N'&text=' + @exchangeCode
																	+ ' Ошибка при исполнении приказа на отмену в АИС; orderID_AIS=' + @orderID_AIS + '; ' + @respMessage
												insert into dbo.logEvents(objectName, messageText)
												  values ('fixDataUpdateNew, callCompleteRejectOrder, Telegram', @messageText)
												EXEC [dbo].[httpGet] @messageText

												return --add 17.09.2021
											end try
											begin catch
											end catch
								
									end try
									begin catch
										insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, callCompleteRejectOrder', 'dbo.httpGetResponse; @urlAddr=' + @urlAddr + '; ' + ERROR_MESSAGE())
										return --add 17.09.2021
									end catch  
							end try
							begin catch						
							end catch


				
					end

				--блок проверки на то, что рынок сейчас открыт
				begin try 			

					select @country = case 
											when @bloom_exchCode in ('NSDQ','NYSE','AMEX') then 'US'
											when (charindex('LSE',@bloom_exchCode)>0 or @bloom_exchCode in ('CHIX')) then 'UK'
											when @exchangeCode in ('AIX','AIXMM') then 'AIX'
											when @exchangeCode in ('KASE') then 'KZ'
											else ''
										end
					
					if dbo.ThisIsWorkDayCountry(dateadd(MINUTE,@diffMinutes,getdate()),@country) <> 1	--разница в минутах (отрицательное значение уже задано в БД)
						--проверка на выходной в заданной стране
						begin
							--insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), isnull(@tickerNew,'') + ' выходной день - не обрабатываем рыночный')
							set @resCode = '-1'
							set @resultString = isnull(@tickerNew,'') + ' выходной день - не обрабатываем рыночный'
							return
						end
					
					--для проверки на время работы бирж используем время UTC
					
					set @startTime = convert(datetime, convert(varchar, dateadd(MINUTE,@diffMinutes,getdate()), 23) 
									+ ' ' + case 
												when @country = 'US' then dbo.getSettingsTP('US_exchange_start_time')
												when @country = 'UK' then dbo.getSettingsTP('UK_exchange_start_time')
												when @exchangeCode = 'KASE' then dbo.getSettingsTP('KASE_T2_exchange_start_time')
												when @exchangeCode = 'AIX' then dbo.getSettingsTP('AIX_exchange_start_time')
											end, 120);
					set @endTime = convert(datetime, convert(varchar, dateadd(MINUTE,@diffMinutes,getdate()), 23) 
									+ ' ' + case 
												when @country = 'US' then dbo.getSettingsTP('US_exchange_end_time')
												when @country = 'UK' then dbo.getSettingsTP('UK_exchange_end_time')
												when @exchangeCode = 'KASE' then dbo.getSettingsTP('KASE_T2_exchange_end_time')
												when @exchangeCode = 'AIX' then dbo.getSettingsTP('AIX_exchange_end_time')
											end, 120);
					
					--при этом приказы на отмену, а также лимитные заказы начинаем посылать за 5 минут до начала открытия соответствующей биржи
					if (@typeLimit in ('1','LIMIT') and @direction in ('1', 'BUY', '2', 'SELL')) or @direction in ('3','CANCEL_BUY','4','CANCEL_SELL')
						set @startTime = DATEADD(MINUTE,-5,@startTime)
								

					--проверяем на время работы бирж
					--проверяем на попадание текущего времени в диапазон разрешенного времени для отправки заявок 
					if dateadd(MINUTE,@diffMinutes,getdate()) not between @startTime and @endTime 
						--and @orderID_AIS <> 545429
						begin
							--insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), isnull(@tickerNew,'') + ' рынок не открыт - не обрабатываем ')
							set @resCode = '-1'
							set @resultString = isnull(@tickerNew,isnull(@ticker,'')) + ' рынок не открыт - не обрабатываем'
							return
						end
						
				end try
				begin catch
					set @resCode = '-1'
					return
				end catch
				--конец блока проверки на то, что рынок сейчас открыт

				if @exchangeCode in ('Mubasher')
					begin				
							set @aggrAccNumMubasher = case 
															when @clientid = 3118226 then case @currency				--реальный Нурдаулет
																							when 'GBP' then 'P000425479'
																							else 'P000425480'
																						end
															when @clientid = 3114529 then case 				--тестовый Тимур --тестовый LY	P000072930
																							when @currency = 'GBP' then 'P000452077'  --'P000072934' - тестовый фунтовый
																							when @currency = 'EUR' then 'P000453940'
																							when @currency = 'USD' and @bloom_exchCode = 'LSE' then 'P000470106' --для долларового LSE отдельный счет 
																							else 'P000452076'			--'P000072930' --тестовый долларовы
																						end												
															else case 											--остальные клиенты 
																	when @currency = 'GBP' then 'P000452077'						--вставить реальные счета !!!!
																	when @currency = 'EUR' then 'P000453940'
																	when @currency = 'USD' and @bloom_exchCode = 'LSE' then 'P000470106' --для долларового LSE отдельный счет 
																	else 'P000452076' --остальные долларовые
																end
														end
													--set @aggrAccNumMubasher = 'P000425480' --'P000425479' --реальный Нурдаулет
													--set @aggrAccNumMubasher = '' --реальный аггрегированный клиентский	
													--USD P000452076
													--GBP P000452077

					end

				else if @exchangeCode in ('AIX','AIXMM') and @brokerCode = 'BCC' and @isMMorder = '1'
					set @tickerNew = @ticker

				else
					--остальные кроме Мубашера
					begin try
						--вычисляем тикер и board
						select top 1 @tickerNew = shortName, @board = board
							 from [drivers_beSQL_new].[dbo].[instrsNew]
							  where nin = @isin
								and isBlocked = 0
								and exchangeCode = @exchangeCode
							order by idObject

						if @tickerNew is null
							begin try 
								insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew', 'select from instrsNew @isin=' + @isin + ' не смогли определить инструмент в instrsNew')
								set @resCode = '-1'
								set @resultString = 'Не смогли определить инструмент в instrsNew'
								return
							end try
							begin catch
								set @resCode = '-1'
								set @resultString = ERROR_MESSAGE()
								return
							end catch
					end try
					begin catch
						set @resCode = '-1'
						insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew', 'select from tickers @isin=' + isnull(@isin,'') + '; ' + ERROR_MESSAGE())
						set @resultString = ERROR_MESSAGE()
						return
					end catch

					
    
				INSERT INTO [dbo].[newOrders]
				   ([isReal]
				   ,[orderID_AIS]
				   ,[origClOrderID]
				   ,[clientID]
				   ,[clientName]
				   ,[exchangeCode]
				   ,[ticker]
				   ,[board]
				   ,[bloom_exchCode]
				   ,[isin]
				   ,[direction]
				   ,[price]
				   ,[quantity]
				   ,[type]
				   ,[acc]
				   ,[investor]
				   ,[isMMorder]
				   ,[timeInForce]
				   ,[expirationDate]
				   ,[currency]
				   ,[handlinst]
				   ,[orderNumberAIS]
				   ,[orderDateAIS]
				   ,[registeredDateAIS]
				   ,[SenderSubID])

				   --"order_date":"2021-02-04","registered_date":"2021-02-04 02:09:32" duration_date":"2021-02-03",
				VALUES
				   (@isReal
				   ,@orderID_AIS
				   ,@rejectingOrderID
				   ,@clientid
				   ,@clientName
				   ,@exchangeCode
				   ,@tickerNew
				   ,@board
				   ,isnull(@bloom_exchCode,@exchangeCode)
				   ,@isin
				   ,case @direction when '1' then 'BUY' when '2' then 'SELL' when '3' then 'CANCEL_BUY' when '4' then 'CANCEL_SELL' else @direction end
				   ,case @currency when 'GBP' then cast(replace(@price,',','.') as decimal(18,6))*100 else cast(replace(@price,',','.') as decimal(18,6)) end  --если это фунты, то переводим в пенсы
				   ,cast(replace(@quantity,',','.') as decimal(18,6))
				   ,case @typeLimit when '1' then 'LIMIT' when '2' then 'MARKET' else @typeLimit end
				   ,case 
						when @exchangeCode in ('Mubasher') then @aggrAccNumMubasher 
						when (@exchangeCode in ('AIX') and @brokerCode = 'Jysan') then 'T01CL'
						else @acc 
					end -- as acc
				   ,case 
						when (@exchangeCode in ('AIX') and @brokerCode = 'Jysan') then case 
																							when charindex('01-',@investor) > 0 then SUBSTRING(@investor,5, 20)
																							else @investor
																						end

						else @investor
				   end -- as investor
				   ,isnull(@isMMorder,'0')
				   ,case 
						 --если это рыночный приказ для AIX, то ставим 'Immediate or Cancel'
						 when @typeLimit in ('2','MARKET') and @exchangeCode = 'AIX' then 'IMMEDIATE_OR_CANCEL'						 
						 --если срок действия "сегодня", то ставим 'DAY'
						 when convert(date, @expirationDate, 23) = cast(getDate() As Date) then 'DAY'								
						 --если это вчерашняя дата приказа и срок действия "вчера", но приказ введен в АИС в течение 3:15 (195 минут) от начала сегодняшнего дня, то ставим 'DAY'
						 when (convert(date, @expirationDate, 23) = dateadd(DAY,-1,cast(getDate() As Date)) 
							  and convert(date, @orderDateAIS, 23) = dateadd(DAY,-1,cast(getDate() As Date))
							  and datediff(MINUTE,cast(getDate() as date),convert(datetime, @inputDateAIS, 121)) < 195)	then 'DAY'			
						 --если срок действия больше чем 25 дней, то ставим'GTC' - до отмены
						 when datediff(DAY,cast(getDate() As Date),convert(date, @expirationDate, 23)) > 25 then 'GOOD_TILL_CANCEL'	
						 --иначе ставим "до даты" 'GOOD_TILL_DATE'
						 else 'GOOD_TILL_DATE' 
					end  --признак срока действия приказа
				   ,convert(date, @expirationDate, 23) --cast(getdate() as date) --
				   ,replace(@currency,'GBP','GBX')	--заменяем фунты на пенсы
				   ,case @exchangeCode when 'Mubasher' then '1' when 'Bloomberg' then '1' else '0' end	--признак автоматической обработки приказа
				   ,@orderNumberAIS
				   ,convert(date, @orderDateAIS, 23)
				   ,convert(datetime, @inputDateAIS, 121)
				   ,@userName)

				if @@ROWCOUNT > 0
					select @resCode = '0', @resultString = 'Заявка записана в newOrders'

			end try
			begin catch
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into newOrders, @isReal=' + cast(@isReal as char(1)), ERROR_MESSAGE())
				set @resCode = '-1'
				set @resultString = ERROR_MESSAGE()
				return
			end catch

	--конец блока если это заявка из АИС для отправки на FIX
	end

else if upper(@objectType) = 'FIX_ORDER' and upper(@typeQuery) = 'UPDATE'
--если это информация о заявке из FIX
	begin
		
		SELECT @serial = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'serial';	
		SELECT @orderReferenceExchange = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'orderReferenceExchange';	
		SELECT @dealNumber = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'dealNumber';	
		SELECT @orderID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'clOrdID';
		SELECT @origClOrderID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'origClOrdID';
		SELECT @ticker = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'instr';
		SELECT @board = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'sessionId';
		SELECT @acc = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'acc';
		SELECT @investor = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'investor';
		SELECT @typeLimit = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'type';
		SELECT @leavesQty = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'leavesQty';
		SELECT @price = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'price';
		SELECT @priceDeal = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'priceDeal';
		SELECT @priceAvg = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'priceAvg';
		SELECT @quantity = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'qty';
		SELECT @quantityDeal = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'qtyDeal';
		SELECT @quantityTotal = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'qtyTotal';
		SELECT @currency = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'currency';
		SELECT @direction = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'side';
		SELECT @executionTime = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'executionTimeStr';
		SELECT @orderStatus = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'status';
		SELECT @expirationDate = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'expireDate';
		SELECT @isMMorder = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'isMMOrder';
		SELECT @userName = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'userName';
		SELECT @timeInForce = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'timeInForce';
		SELECT @settlementDateStr = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'settlementDateStr';
		SELECT @comments = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'comment';
		

		if not exists (select Id from [Orders] 
						where serial = @serial   COLLATE SQL_Latin1_General_CP1_CS_AS
						  and [isReal] = @isReal
						  and [clientOrderID] = @orderID	--add 26.02.2021
						  --and datediff(DAY,convert(date, @executionTime, 121),cast([executionTime] As Date)) between 0 and 7 del.26.02.2021   , а также что это не старый отчет (в течение недели)
						  )
			--проверяем, нет ли уже такого отчета в таблице Orders
			begin
				begin try
				
					INSERT INTO [dbo].[orders]
						([isReal]
						,[clientOrderID]
						,[origClOrderID]
						,[serial]
						,[orderReferenceExchange]
						,[sessionId]
						,[ticker]
						,[board]
						,[direction]
						,[price]
						,[priceDeal]
						,[priceAvg]
						,[quantity]
						,[quantityDeal]
						,[quantityDealTotal]
						,[leavesQty]
						,[volume_Cash]
						,[currency]
						,[status]
						,[type]
						,[comments]
						,[acc]
						,[investor]
						--,[isMMorder]
						,[UserName]
						,[timeInForce]
						,[executionTime]
						,[expirationDate]
						,[settlementDate]
						)
					VALUES 
						(@isReal
						,@orderID
						,@origClOrderID
						,@serial
						,@orderReferenceExchange
						,@dealNumber
						,@ticker				
						,@board
						,@direction
						,cast(replace(@price,',','.') as decimal(18,6))
						,cast(replace(@priceDeal,',','.') as decimal(18,6))
						,cast(replace(@priceAvg,',','.') as decimal(18,6))
						,cast(replace(@quantity,',','.') as decimal(18,6))
						,cast(replace(@quantityDeal,',','.') as decimal(18,6))
						,cast(replace(@quantityTotal,',','.') as decimal(18,6))
						,cast(replace(@leavesQty,',','.') as decimal(18,6))
						,null
						,@currency
						,@orderStatus
						,@typeLimit
						,@comments
						,@acc
						,@investor
						--,case upper(@isMMorder) when 'TRUE' then 1 when 'FALSE' then 0 else @isMMorder end
						,@userName
						,@timeInForce
						,convert(datetime, @executionTime, 121)
						,null--convert(datetime, @expirationDate, 107)
						,@settlementDate
						)
			
					select @newOrder = SCOPE_IDENTITY()
		
				end try
				begin catch
					insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into orders, @isReal=' + cast(@isReal as char(1)), ERROR_MESSAGE())
					set @resCode = '-1'
					set @resultString = ERROR_MESSAGE()
					return
				end catch

				if isnull(@newOrder,0) > 0 
					begin try
						update ord
							set ord.clientID = n.clientID
							,ord.orderID_AIS = n.orderID_AIS
							,ord.exchangeCode = n.exchangeCode
							,ord.bloom_exchCode = n.bloom_exchCode
							,ord.currency = n.currency
							,ord.clientName = n.clientName
							,ord.isMMorder = n.isMMorder
							from orders ord, newOrders n
							where ord.clientOrderID = n.id
							  and ord.clientOrderID = @orderID
				
						if charindex('FILL',@orderStatus) > 0
							begin try					
								--рассчитываем комиссию контрагента
								select @commissionContragent = case
																	when currency in ('USD') then 0.0005 * cast(replace(@priceDeal,',','.') as decimal(18,6)) * cast(replace(@quantityDeal,',','.') as decimal(18,6))
																	when currency in ('GBP')  then 0.001 * cast(replace(@priceDeal,',','.') as decimal(18,6)) * cast(replace(@quantityDeal,',','.') as decimal(18,6))
																	else 0
																end
									from newOrders 
									 where id = @orderID

								update orders
								 set commissionContragent = @commissionContragent
								 where id = @newOrder

							end try
							begin catch
								insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, update orders, @isReal=' + cast(@isReal as char(1)), 'calc commissionContragent - ' + ERROR_MESSAGE())
							end catch

						--пытаемся высчитать дату расчетов
						begin try
							if isnull(@settlementDateStr,'') <> ''
								update orders
								 set settlementDate = convert(date, substring(@settlementDateStr,1,4)+'-'+substring(@settlementDateStr,5,2)+'-'+substring(@settlementDateStr,7,2), 23)
								 where id = @newOrder
							else
								update orders
								set settlementDate = dbo.GetNextWorkDayCountry(convert(date, @executionTime, 121),
																							2, 
																							case 
																							when bloom_exchCode in ('NYSE','NSDQ','AMEX') then 'US'
																							when charindex('LSE',bloom_exchCode) > 0 then 'UK'
																							else 'KZ'
																							end)
								where id = @newOrder
						end try
						begin catch
							begin try
								update orders
								set settlementDate = dbo.GetNextWorkDayCountry(convert(date, @executionTime, 121),
																								2, 
																								case 
																								when bloom_exchCode in ('NYSE','NSDQ','AMEX') then 'US'
																								when charindex('LSE',bloom_exchCode) > 0 then 'UK'
																								else 'KZ'
																								end)
								where id = @newOrder
							end try
							begin catch
								update orders
								 set settlementDate = convert(date,DATEADD(DAY,5,getdate()))
								 where id = @newOrder
							end catch
						end catch

					end try
					begin catch
						insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, update orders, @isReal=' + cast(@isReal as char(1)), ERROR_MESSAGE())
						set @resCode = '-1'
						set @resultString = ERROR_MESSAGE()
						return
					end catch

				if @newOrder > 0
					select @resCode = '0', @resultString = 'Заявка записана в orders'		

				--отправляем в Телеграм сигнал об отклонении приказа 19.04.2021		
				if @orderStatus	= 'REJECTED'
					begin try
						select @messageText = @urlTelegram + N'&text='
											+ 'Отказ ' + isnull(exchangeCode,'???') + ' в принятии приказа; orderID_AIS=' + orderID_AIS + '; ' + isnull(@comments,'')
							from orders
							 where id = @newOrder

						insert into dbo.logEvents(objectName, messageText)
							values ('fixDataUpdateNew, Telegram', @messageText)
						EXEC [dbo].[httpGet] @messageText
					end try
					begin catch
						insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, Telegram', ERROR_MESSAGE())
					end catch


			    --отправляем на сервис банка
                begin try
                    --declare @urlAddr		nvarchar(MAX)
                    --declare @return_value	int
                    --declare @respCode		varchar(10)
                    --declare @respMessage	varchar(max)
                    declare @len int
                    declare @myid uniqueidentifier = NEWID(); 

                    --проставляем отметку об исполнении приказа на отмену
                    set @urlAddr = dbo.getSettingsTP('dbpAddr')
                    set @stringTmp = (select * from orders where id = @newOrder FOR JSON PATH)
                    set @len = len(@stringTmp)
                    set @stringTmp = substring(substring(@stringTmp,2,@len),1,@len-2)

                    select @messageText = '{
											"records": [
												{
													"value": {
														"meta": {
															"event": {
																"module": "' + dbo.getSettingsTP('dbpModule') + '",
																"id": "' + lower(CONVERT(CHAR(255), @myid)) + '",
																"correlationKey": "' + orderID_AIS + '",
																"time": ' + convert(varchar,DATEDIFF_BIG(ms, '1970-01-01 00:00:00', getdate())) + ',
																"code": "' + dbo.getSettingsTP('dbpCode') + '"
															}
														},
														"payload": ' + @stringTmp + '
													}
												}
											]
										}'
                    from orders
                    where id = @newOrder

                    insert into dbo.logEvents(objectName, messageText)
                    values ('fixDataUpdateNew, dbpAddr', @urlAddr + ' ' + @messageText)

                    EXEC @return_value = [dbo].[httpPostResponse]
                                         @url = @urlAddr,
                                         @contentType = 'application/vnd.kafka.json.v2+json; charset=utf-8',
                                         @postData = @messageText,
                                         @respCode = @respCode OUTPUT,
                                         @respMessage = @respMessage OUTPUT --ответ от сервиса

                    insert into dbo.logEvents(objectName, messageText)
                    values ('fixDataUpdateNew, dbpAddr', isnull(@respCode,'') + ' ' + isnull(@respMessage,''))

                    if @respCode = '200'
                        update orders
                        set sendToAIS = 1, sendToAIStime = getdate()
                        where id = @newOrder

                end try
                begin catch
                    insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew', 'dbo.httpPostResponse; @urlAddr=' + @urlAddr + '; ' + @stringTmp + '; ' + ERROR_MESSAGE())
                end catch

            end
		else
			begin
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into orders, @isReal=' + cast(@isReal as char(1)), 'Повторный ExecutionReport - ' + @serial)
				set @resCode = '-1'
				set @resultString = 'Повторный ExecutionReport - ' + @serial
				return
			end
	--конец блока если это информация о заявке из FIX
	end
		
else if upper(@objectType) = 'FIX_ORDER' and upper(@typeQuery) = 'CANCELREJECT'
--если это ответ от биржи об отклонении заявки на отмену приказа
	begin
		

		SELECT @orderID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'clOrdID';
		SELECT @origClOrderID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'origClOrdID';
		SELECT @orderStatus = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'status';
		SELECT @userName = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'userName';
		SELECT @comments = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'comment';

		--{"isReal":"1","clOrdID":"78","origClOrdID":"77","status":"REJECTED","comment":"Rejected:Invalid Tag 41 (DIFC)","userName":"FIX.4.2"}
		
		begin try
				
			INSERT INTO [dbo].[orders]
				([isReal]
				,[clientOrderID]
				,[origClOrderID]
				,[clientID]
				,[orderID_AIS]
				,[serial]
				,[ticker]
				,[board]
				,[exchangeCode]
				,[bloom_exchCode]
				,[direction]
				,[price]
				,[priceDeal]
				,[priceAvg]
				,[quantity]
				,[quantityDeal]
				,[quantityDealTotal]
				,[leavesQty]
				,[volume_Cash]
				,[status]
				,[type]
				,[comments]
				,[acc]
				,[investor]
				,[isMMorder]
				,[UserName]
				,[timeInForce]
				,[executionTime]
				,[expirationDate]
				,[clientName]
				)
			SELECT 
				@isReal
				,@orderID
				,@origClOrderID
				,clientID
				,orderID_AIS
				,null
				,ticker				
				,board
				,exchangeCode
				,bloom_exchCode
				,direction
				,price
				,null
				,null
				,quantity
				,null
				,null
				,null
				,null
				,'REJECTED'-- @orderStatus	--upd.17.02.2021
				,[type]
				,@comments
				,acc
				,investor
				,isMMorder
				,@userName
				,null
				,null --convert(datetime, @executionTime, 107)
				,null--convert(datetime, @expirationDate, 107)
				,clientName
			 from newOrders
			  where id = @orderID
			
			select @newOrder = SCOPE_IDENTITY()
		
		end try
		begin catch
			insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into orders CancelReject, @isReal=' + cast(@isReal as char(1)), ERROR_MESSAGE())
			set @resCode = '-1'
			set @resultString = ERROR_MESSAGE()
			return
		end catch		

		if @newOrder > 0
			select @resCode = '0', @resultString = 'Заявка записана в orders'


		--отправляем в Телеграм сигнал об отклонении приказа 19.04.2021					
		begin try
			select @messageText = @urlTelegram + '&text=' + isnull(exchangeCode,'???')
								+ N' Отказ в принятии приказа на отмену; orderID_AIS=' + orderID_AIS + '; ' + isnull(@comments,'')
				from orders
				where id = @newOrder

			insert into dbo.logEvents(objectName, messageText)
				values ('fixDataUpdateNew, Telegram', @messageText)
			EXEC [dbo].[httpGet] @messageText
		end try
		begin catch
			insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, Telegram', ERROR_MESSAGE())
		end catch
	--конец блока если это ответ от биржи об отклонении заявки на отмену приказа
	end

else if upper(@objectType) in ('INSTRS','QUOTE') and upper(@typeQuery) = 'UPDATE'
	--если это обновление инструмента или котировки (например, для AIX)
	begin
		SELECT @exchangeCode = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'exchangeCode';
		SELECT @ticker = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'ticker';
				
		if upper(@objectType) = 'INSTRS'
			begin
				SELECT @currency = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'currencyCode';
				SELECT @priceAvg = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'TradeRefPrice';	--цена для ориентира (пока не используем?)
				SELECT @tradingSessionSubID = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'tradingSessionSubID';
				SELECT @tradSesStatus = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'tradSesStatus';
				SELECT @tradSesStartTime = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'tradSesStartTime';
				SELECT @tradText = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'tradText';
			end
		else if upper(@objectType) = 'QUOTE'
			begin
				SELECT @direction = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'direction';
				SELECT @price = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'price';	
				SELECT @quantity = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'quantity';	
				SELECT @typeLimit = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'best';	
				SELECT @orderStatus = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'updateAction';	--для удаления стакана по инструменту (J)
				SELECT @msgType = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'msgType';
				SELECT @msgNum = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'msgNum';
				SELECT @sendingTime = replace(value,'null','') FROM dbo.fnSplitJson2(@messageText, NULL) where name = 'sendingTime';
			end
		
		--записываем в локальные таблицы
		if upper(@objectType) = 'INSTRS'
			begin try
				insert into instruments (isReal, exchangeCode, ticker, currencyCode, TradeRefPrice, TradingSessionSubID, TradSesStatus, TradSesStartTime, TradText)
					values (@isReal, @exchangeCode, @ticker, @currency, @priceAvg, @tradingSessionSubID, @tradSesStatus, @tradSesStartTime, @tradText)
			end try
			begin catch
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into instruments', ERROR_MESSAGE())
				set @resCode = '-1'
			end catch	
		else if isnull(@orderStatus,'') = 'J' and upper(@objectType) = 'QUOTE'
			begin try
				insert into quotes(isReal, exchangeCode, ticker, direction, price, volume)
					values (@isReal, @exchangeCode, @ticker, 'CLEAR QUOTES ' + isnull(@direction,''), 0, 0)
			end try
			begin catch
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, CLEAR QUOTES', ERROR_MESSAGE())
				set @resCode = '-1'
			end catch
		else if upper(@objectType) = 'QUOTE'
			begin try
				insert into quotes(isReal, exchangeCode, ticker, direction, price, volume, msgType, msgNum, sendingTime)
					values (@isReal, @exchangeCode, @ticker, @direction, @price, @quantity, @msgType, @msgNum, @sendingTime)
			end try
			begin catch
				insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, insert into quotes', ERROR_MESSAGE())
				set @resCode = '-1'
			end catch
		
		if @isReal in (1) -- потом убрать 0!!
			begin
				begin try

					if isnull(@orderStatus,'') = 'J' and upper(@objectType) = 'QUOTE' and @direction in ('0','BID','1','OFFER')
						set @stringTmp = '{"objectType":"GLASS","typeQuery":"UPDATE","updateAction":"J","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker 
						+ '",' + case when @direction in ('0','BID') then '"bidQuantity":"-1"' when @direction in ('1','OFFER') then '"askQuantity":"-1"' else '' end  + '}'
		 
					else if upper(@objectType) = 'INSTRS'  
						set @stringTmp = '{"objectType":"INSTRS","typeQuery":"UPDATE","updateAction":"IDF","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker + '"'
							+ case 
								when isnull(@tradSesStatus,'') = '' then ',"TradeRefPrice":"' + @priceAvg + '","tradingCurrency":"' + @currency + '"}'
								else  ',"tradeStatus":"' + case when @tradSesStatus in ('2','5') then 'open' else 'close' end + '"}'
							  end
					else if isnull(@orderStatus,'') = 'FINAL' and upper(@objectType) = 'QUOTE' and @direction in ('B','TRADE_VOLUME') --and @msgType = 'MDFullSnapshotRefresh'
						--если это конец записи снапшота по объемам сделок, то высчитываем общую сумму, цену последней сделки, а также цену открытия и минимум/максимум
						begin try
							declare @openPrice decimal(18,8)
							declare @lastPrice decimal(18,8)
							declare @minPrice decimal(18,8)
							declare @maxPrice decimal(18,8)
							declare @volTotal decimal(18,8)
							
							select top 1 @openPrice = price from quotes 
								where isReal = @isReal and exchangeCode = @exchangeCode and ticker = @ticker and direction = 'TRADE_VOLUME' and msgType = 'MDFullSnapshotRefresh'
								  and msgNum = @msgNum and sendingTime = @sendingTime
								  order by ID
							
							select top 1 @lastPrice = price from quotes 
								where isReal = @isReal and exchangeCode = @exchangeCode and ticker = @ticker and direction = 'TRADE_VOLUME' and msgType = 'MDFullSnapshotRefresh'
								  and msgNum = @msgNum and sendingTime = @sendingTime
								  order by ID desc

							select @minPrice = min(price) from quotes 
								where isReal = @isReal and exchangeCode = @exchangeCode and ticker = @ticker and direction = 'TRADE_VOLUME' and msgType = 'MDFullSnapshotRefresh'
								  and msgNum = @msgNum and sendingTime = @sendingTime
							
							select @maxPrice = max(price) from quotes 
								where isReal = @isReal and exchangeCode = @exchangeCode and ticker = @ticker and direction = 'TRADE_VOLUME' and msgType = 'MDFullSnapshotRefresh'
								  and msgNum = @msgNum and sendingTime = @sendingTime

							select @volTotal = sum(volume*price) from quotes 
								where isReal = @isReal and exchangeCode = @exchangeCode and ticker = @ticker and direction = 'TRADE_VOLUME' and msgType = 'MDFullSnapshotRefresh'
								  and msgNum = @msgNum and sendingTime = @sendingTime

							set @stringTmp = '{"objectType":"INSTRS","typeQuery":"UPDATE","updateAction":"MSR","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker 
									+ '","openPrice":"' + cast(@openPrice as varchar) + '","lastPrice":"' + cast(@lastPrice as varchar) + '","minPrice":"' + cast(@minPrice as varchar) +
									'","maxPrice":"' + cast(@maxPrice as varchar) + '","volTotal":"' + cast(@volTotal as varchar) + '"}'
						end try
						begin catch
							insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, select FINAL TRADE_VOLUME', ERROR_MESSAGE())
							set @resCode = '-1'
						end catch						
					
					else 
						--по идее это инкрементальные обновления
						begin 
							if @direction in ('OPENING_PRICE','TRADE', 'TRADING_SESSION_HIGH_PRICE', 'TRADING_SESSION_LOW_PRICE')
								set @stringTmp = '{"objectType":"INSTRS","typeQuery":"UPDATE","updateAction":"MSR","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker + '",' 
											+ case 
												when @direction = 'OPENING_PRICE' then '"openPrice":'
												when @direction = 'TRADING_SESSION_HIGH_PRICE' then '"maxPrice":' 
												when @direction = 'TRADING_SESSION_LOW_PRICE' then '"minPrice":' 
												when @direction = 'TRADE' then '"lastPrice":' 
												else ''
											end + '"' + @price + '"}'
							
							else if @direction in ('BID','OFFER') and @typeLimit = '1'
								--если это лучшая цена в стакане, то засылаем бид и аск в инструмент
								set @stringTmp = '{"objectType":"INSTRS","typeQuery":"UPDATE","updateAction":"MSR","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker + '",' 
											+ case 
												when @direction = 'BID' then '"bid":'
												when @direction = 'OFFER' then '"ask":' 
												else ''											
											end + '"' + @price + '"}'
						end

					if isnull(@stringTmp,'') <> ''
						begin
							exec @newOrder = [drivers_beSQL_new].[dbo].[fixDataUpdateNew] 
									@messageText = @stringTmp,		--текст запроса в формате json
									@resCode = @resCode,			--код возврата
									@resultString = @resultString
				
							insert into dbo.xmlLog(functionName, xmlIn)
								values ('fixDataUpdateNew, ' + @exchangeCode, isnull(@stringTmp,''))
						end
				end try
				begin catch
					insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, INSTRS UPDATE', ERROR_MESSAGE())
					set @resCode = '-1'
					--return
				end catch

			
				if upper(@objectType) = 'QUOTE'and @direction in ('BID','OFFER')
					--данные в стакан
					begin try

						set @stringTmp = '{"objectType":"GLASS","typeQuery":"UPDATE","updateAction":"' + case when @quantity = '0' then '2' else '0' end 
									+ '","sourceName":"' + @exchangeCode + '","ticker":"' + @ticker + '","price":"' + @price + '",'
									+ case 
											when @direction = 'BID' then '"bidQuantity":'
											when @direction = 'OFFER' then '"askQuantity":' 
											else ''
										end + '"' + @quantity 
									+ '","best":"' + case @typeLimit when '1' then '1' else '0' end + '"}'

						exec @newOrder = [drivers_beSQL_new].[dbo].[fixDataUpdateNew] 
								@messageText = @stringTmp,		--текст запроса в формате json
								@resCode = @resCode,			--код возврата
								@resultString = @resultString
				
						insert into dbo.xmlLog(functionName, xmlIn)
							values ('fixDataUpdateNew, ' + @exchangeCode, isnull(@stringTmp,''))						
									
					end try
					begin catch
						insert into dbo.logErrors(objectName,errMsg) values ('fixDataUpdateNew, GLASS UPDATE', ERROR_MESSAGE())
						set @resCode = '-1'
						--return
					end catch
			end
		
		
		if isnull(@resultString,'') = ''
			set @resCode = '-1'
		else 
			set @resCode = '0'
		
		return
	end

END
